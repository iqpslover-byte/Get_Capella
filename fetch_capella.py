"""
Capella Open Data STAC クローラー
2024〜2025年の全シーンを収集し、地理的にクラスタリングしてJSONに保存。

JSON構造:
{
  "generated": "ISO8601",
  "total_scenes": N,
  "total_groups": M,
  "groups": [
    {
      "center": [lat, lng],
      "count": 5,
      "latest_geometry": [[lat,lng], ...],
      "scenes": [
        {"datetime": "...", "platform": "...", "tif_href": "...", "thumbnail": "..."}
      ]
    }
  ]
}

外部依存: requests のみ
"""
import json, sys, os
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

STAC_BASE = 'https://capella-open-data.s3.amazonaws.com/stac/capella-open-data-by-datetime/'
TARGET_YEARS = ['2024', '2025']
WORKERS = int(os.environ.get('CAPELLA_WORKERS', '12'))
CLUSTER_DEG = float(os.environ.get('CAPELLA_CLUSTER_DEG', '0.1'))  # ~10km

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'CapellaFetcher/1.0'})
if os.environ.get('CAPELLA_NO_VERIFY', ''):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    SESSION.verify = False


def fetch_json(url, timeout=20):
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def collect_day_urls():
    """全対象年の「日カタログURL」リストを返す。"""
    day_urls = []
    for year in TARGET_YEARS:
        year_url = STAC_BASE + f'capella-open-data-{year}/catalog.json'
        try:
            cat = fetch_json(year_url)
        except Exception as e:
            print(f'  WARN year {year}: {e}', file=sys.stderr)
            continue
        for ml in cat.get('links', []):
            if ml.get('rel') != 'child':
                continue
            month_url = urllib.parse.urljoin(year_url, ml['href'])
            try:
                cat_m = fetch_json(month_url)
            except Exception as e:
                print(f'  WARN month {month_url}: {e}', file=sys.stderr)
                continue
            for dl in cat_m.get('links', []):
                if dl.get('rel') != 'child':
                    continue
                day_url = urllib.parse.urljoin(month_url, dl['href'])
                day_urls.append(day_url)
    return day_urls


def fetch_day(day_url):
    """1日分のカタログから全シーンのメタデータを返す。"""
    scenes = []
    try:
        cat_d = fetch_json(day_url)
    except Exception as e:
        print(f'  WARN day {day_url}: {e}', file=sys.stderr)
        return scenes

    item_hrefs = [l['href'] for l in cat_d.get('links', []) if l.get('rel') == 'item']
    for href in item_hrefs:
        item_url = urllib.parse.urljoin(day_url, href)
        try:
            item = fetch_json(item_url)
        except Exception as e:
            print(f'    WARN item {item_url}: {e}', file=sys.stderr)
            continue

        props = item.get('properties', {})
        assets = item.get('assets', {})
        geom = item.get('geometry', {})

        thumbnail = assets.get('thumbnail', {}).get('href', '')
        dt = props.get('datetime', '')
        platform = props.get('platform', '')

        # GEO TIF URL を thumbnail から生成（_thumb.png → .tif）
        # thumbnailはGEOフォルダを指すため、GEOのTIFが確実に取得できる
        # GECはGeoTIFF.jsで読めないため使わない
        if thumbnail and '_thumb.png' in thumbnail:
            tif_href = thumbnail.replace('_thumb.png', '.tif')
        else:
            tif_href = assets.get('HH', {}).get('href', '')

        # GeoJSON座標 [lng,lat] → [lat,lng] に変換
        coords_raw = geom.get('coordinates', [[]])[0] if geom.get('type') == 'Polygon' else []
        geometry = [[round(c[1], 6), round(c[0], 6)] for c in coords_raw]

        if not tif_href or not geometry:
            continue

        scenes.append({
            'datetime':  dt,
            'platform':  platform,
            'tif_href':  tif_href,
            'thumbnail': thumbnail,
            'geometry':  geometry,
        })
    return scenes


def get_center(geometry):
    """ポリゴン座標リスト[[lat,lng],...]の重心を返す。"""
    lats = [p[0] for p in geometry]
    lngs = [p[1] for p in geometry]
    return [sum(lats) / len(lats), sum(lngs) / len(lngs)]


def cluster_scenes(scenes, threshold=CLUSTER_DEG):
    """中心が threshold 度以内のシーンを同グループにまとめる（貪欲法）。"""
    used = [False] * len(scenes)
    groups = []

    for i, sc in enumerate(scenes):
        if used[i]:
            continue
        c0 = get_center(sc['geometry'])
        group = [sc]
        used[i] = True

        for j in range(i + 1, len(scenes)):
            if used[j]:
                continue
            c1 = get_center(scenes[j]['geometry'])
            dist = ((c0[0] - c1[0]) ** 2 + (c0[1] - c1[1]) ** 2) ** 0.5
            if dist < threshold:
                group.append(scenes[j])
                used[j] = True

        # datetime 昇順でソート（最後が最新）
        group.sort(key=lambda s: s['datetime'])
        centers = [get_center(s['geometry']) for s in group]
        avg_lat = sum(c[0] for c in centers) / len(centers)
        avg_lng = sum(c[1] for c in centers) / len(centers)

        groups.append({
            'center':          [round(avg_lat, 5), round(avg_lng, 5)],
            'count':           len(group),
            'latest_geometry': group[-1]['geometry'],
            'scenes':          group,
        })

    return groups


def main():
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'capella_scenes.json')

    print(f'対象年: {TARGET_YEARS}  クラスタ閾値: {CLUSTER_DEG}度')
    print('日カタログURLを収集中...')
    day_urls = collect_day_urls()
    print(f'{len(day_urls)}日分のカタログを発見\n')

    all_scenes = []
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_day, url): url for url in day_urls}
        for fut in as_completed(futs):
            try:
                scenes = fut.result()
            except Exception as e:
                print(f'  ERR {futs[fut]}: {e}', file=sys.stderr)
                scenes = []
            all_scenes.extend(scenes)
            done += 1
            if done % 50 == 0 or done == len(day_urls):
                print(f'  [{done}/{len(day_urls)}] 累計 {len(all_scenes)} シーン')

    # 日時でソートしてからクラスタリング
    all_scenes.sort(key=lambda s: s['datetime'])
    print(f'\nクラスタリング中 ({len(all_scenes)}シーン)...')
    groups = cluster_scenes(all_scenes)
    print(f'{len(groups)}グループに集約')

    out = {
        'generated':    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'total_scenes': len(all_scenes),
        'total_groups': len(groups),
        'groups':       groups,
    }
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    print(f'完了: {len(all_scenes)}シーン / {len(groups)}グループ → {out_path}')


if __name__ == '__main__':
    main()
