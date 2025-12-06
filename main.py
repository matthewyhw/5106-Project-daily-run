import os
import time
import json
import re
import math
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


# ========================= CONFIG =========================
VACANCY_API = "https://resource.data.one.gov.hk/td/carpark/vacancy_all.json"
CARPARK_INFO_API = "https://resource.data.one.gov.hk/td/carpark/basic_info_all.json"

DAYS = 28
INTERVAL_MINUTES = 30

DRY_RUN = True          # 測試時 True，上 GitHub 想抓全量就改 False
LIMIT_SNAPSHOTS = 6     # DRY_RUN 時只抓最後 N 個時間點
DRY_MAX_PARKS = 20      # DRY_RUN 時只保留部分 car park

REQUEST_SLEEP = 0.1

VACANCY_CACHE_DIR = "./carpark_cache"
INFO_CACHE_DIR = "./carpark_merge_cache"
os.makedirs(VACANCY_CACHE_DIR, exist_ok=True)
os.makedirs(INFO_CACHE_DIR, exist_ok=True)

SPREADSHEET_ID = "1KsHTcbvVRR9w252DW3vfabRu5iUf-HEvzp4CeWs2UAk"
SHEET_NAME = "data"

# ========================= 共用 requests session =========================
session = requests.Session()
session.headers.update({"User-Agent": "carpark-collector-merge/1.0"})

def get_with_retry(url, params=None, timeout=30, tries=3, backoff=1.0):
    last_exc = None
    for attempt in range(tries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if attempt == tries - 1:
                break
            time.sleep(backoff * (1 + attempt))
    if last_exc:
        raise last_exc

# ========================= vacancy：時間序列抓取 =========================
def round_down_to_half_hour(dt):
    minute = 0 if dt.minute < 30 else 30
    return dt.replace(minute=minute, second=0, microsecond=0)

def generate_timestamps(days=DAYS):
    end = round_down_to_half_hour(datetime.utcnow())
    start = end - timedelta(days=days)
    ts = []
    cur = start
    while cur <= end:
        ts.append(cur)
        cur += timedelta(minutes=INTERVAL_MINUTES)
    return ts

def vacancy_cache_path(ts):
    return os.path.join(VACANCY_CACHE_DIR, ts.strftime("%Y%m%d%H%M") + ".json")

def try_direct_api(ts):
    formats = [
        lambda d: d.strftime("%Y-%m-%dT%H:%M:%S"),
        lambda d: d.strftime("%Y%m%d%H%M%S"),
        lambda d: d.strftime("%Y-%m-%dT%H:%M"),
    ]
    param_names = ["t", "time", "timestamp", "date"]

    for fmt in formats:
        s = fmt(ts)
        for p in param_names:
            try:
                r = get_with_retry(VACANCY_API, params={p: s}, tries=2)
                if r and r.text.strip():
                    content_type = r.headers.get('content-type', '')
                    if r.text.strip().startswith(('{', '[')) or 'json' in content_type:
                        return r.text
            except Exception:
                pass
            time.sleep(0.05)

    try:
        r = get_with_retry(VACANCY_API, tries=2)
        if r and r.text.strip():
            return r.text
    except Exception:
        pass
    return None

def try_wayback(ts):
    try:
        ts_str = ts.strftime("%Y%m%d%H%M%S")
        r = get_with_retry(
            "http://archive.org/wayback/available",
            params={"url": VACANCY_API, "timestamp": ts_str},
            tries=3,
        )
        j = r.json()
        snap = j.get("archived_snapshots", {}).get("closest")
        if not snap or not snap.get("available"):
            return None
        snap_url = snap["url"]
        r2 = get_with_retry(snap_url)
        txt = r2.text
        start = min(i for i in (txt.find("{"), txt.find("[")) if i != -1)
        return txt[start:]
    except Exception:
        return None

def fetch_snapshot(ts):
    cache_file = vacancy_cache_path(ts)
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return f.read()

    txt = try_direct_api(ts) or try_wayback(ts)
    if txt:
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(txt)
        return txt
    return None

def flatten_carpark_record(rec, snapshot_ts):
    rows = []
    raw_json = json.dumps(rec, ensure_ascii=False)

    park_id = None
    for key in ("park_id", "ParkID", "carpark_id", "carpark_no", "id"):
        if key in rec:
            park_id = rec[key]
            break
    if park_id is None:
        for k in rec:
            if "park" in k.lower() or ("id" in k.lower() and len(k) <= 10):
                park_id = rec.get(k)
                break

    vehicle_types = rec.get("vehicle_type") or rec.get("vehicleType") or []
    if not isinstance(vehicle_types, list):
        vehicle_types = [vehicle_types] if vehicle_types else []

    if not vehicle_types:
        rows.append({
            "snapshot_requested_utc": snapshot_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "park_id": park_id,
            "vehicle_type": None,
            "service_category": None,
            "vacancy_type": None,
            "vacancy": None,
            "lastupdate": None,
            "raw_json": raw_json
        })
        return rows

    for vt in vehicle_types:
        vt_type = vt.get("type") if isinstance(vt, dict) else str(vt)

        service_cats = []
        if isinstance(vt, dict):
            service_cats = vt.get("service_category") or vt.get("serviceCategory") or []
        if not isinstance(service_cats, list):
            service_cats = [service_cats] if service_cats else []

        if not service_cats:
            rows.append({
                "snapshot_requested_utc": snapshot_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "park_id": park_id,
                "vehicle_type": vt_type,
                "service_category": None,
                "vacancy_type": None,
                "vacancy": None,
                "lastupdate": None,
                "raw_json": raw_json
            })
            continue

        for sc in service_cats:
            if not isinstance(sc, dict):
                rows.append({
                    "snapshot_requested_utc": snapshot_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "park_id": park_id,
                    "vehicle_type": vt_type,
                    "service_category": str(sc),
                    "vacancy_type": None,
                    "vacancy": None,
                    "lastupdate": None,
                    "raw_json": raw_json
                })
                continue

            category = sc.get("category") or sc.get("service") or sc.get("type")
            vacancy_type = sc.get("vacancy_type") or sc.get("vacancyType")
            vacancy_raw = sc.get("vacancy")
            lastupdate = sc.get("lastupdate") or sc.get("last_update")

            try:
                vacancy = int(vacancy_raw) if vacancy_raw not in (None, "", "N/A") else None
            except Exception:
                vacancy = None

            rows.append({
                "snapshot_requested_utc": snapshot_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "park_id": park_id,
                "vehicle_type": vt_type,
                "service_category": category,
                "vacancy_type": vacancy_type,
                "vacancy": vacancy,
                "lastupdate": lastupdate,
                "raw_json": raw_json
            })
    return rows

def build_vacancy_df():
    print("Generating timestamps...")
    timestamps = generate_timestamps(DAYS)
    if DRY_RUN:
        timestamps = timestamps[-LIMIT_SNAPSHOTS:]
        print(f"DRY_RUN enabled: using last {LIMIT_SNAPSHOTS} timestamps only")

    print(f"Fetching data for {len(timestamps)} timestamps...")

    all_rows = []
    failed_timestamps = []

    for i, ts in enumerate(timestamps):
        if (i + 1) % 50 == 0 or i < 10 or i == len(timestamps) - 1:
            print(f"  → Processed {i + 1}/{len(timestamps)} timestamps...")

        data = fetch_snapshot(ts)
        if not data:
            failed_timestamps.append(ts)
            continue

        try:
            parsed = json.loads(data)
        except Exception:
            start = min([i for i in (data.find("{"), data.find("[")) if i >= 0] or [0])
            try:
                parsed = json.loads(data[start:])
            except Exception:
                failed_timestamps.append(ts)
                continue

        records = []
        if isinstance(parsed, list):
            records = parsed
        elif isinstance(parsed, dict):
            for key in ("car_park", "data", "records", "results", "carparks", "items"):
                if key in parsed and isinstance(parsed[key], list):
                    records = parsed[key]
                    break

        if not records:
            def find_lists(obj):
                best = []
                if isinstance(obj, list) and len(obj) > len(best) and all(isinstance(x, dict) for x in obj[:5]):
                    best = obj
                elif isinstance(obj, dict):
                    for v in obj.values():
                        sub = find_lists(v)
                        if len(sub) > len(best):
                            best = sub
                elif isinstance(obj, list):
                    for item in obj:
                        sub = find_lists(item)
                        if len(sub) > len(best):
                            best = sub
                return best
            records = find_lists(parsed)

        if not records:
            failed_timestamps.append(ts)
            continue

        for rec in records:
            try:
                rows = flatten_carpark_record(rec, ts)
                all_rows.extend(rows)
            except Exception:
                pass

        time.sleep(0.05)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["snapshot_requested_utc"] = pd.to_datetime(df["snapshot_requested_utc"])
        df["date"] = df["snapshot_requested_utc"].dt.date
        df["hour"] = df["snapshot_requested_utc"].dt.hour
        df["minute"] = df["snapshot_requested_utc"].dt.minute
        df = df.sort_values(
            ["snapshot_requested_utc", "park_id", "vehicle_type", "service_category"]
        ).reset_index(drop=True)

    print("\nVACANCY SUCCESS!")
    print(f"DataFrame shape: {df.shape}")
    print(f"Unique car parks: {df['park_id'].nunique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"Failed timestamps: {len(failed_timestamps)} (out of {len(timestamps)})")

    return df

# ========================= basic info + 價錢解析 =========================
DAY_NAME_MAP = {
    'mon': 0, 'monday': 0,
    'tue': 1, 'tues': 1, 'tuesday': 1,
    'wed': 2, 'wednesday': 2,
    'thu': 3, 'thurs': 3, 'thursday': 3,
    'fri': 4, 'friday': 4,
    'sat': 5, 'saturday': 5,
    'sun': 6, 'sunday': 6
}

_money_re = re.compile(r'(?P<cur>HK\$|\$)\s*(?P<amt>\d{1,3}(?:,\d{3})*(?:\.\d+)?)', re.I)

def time_str_to_minutes(s):
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    if ':' in s:
        parts = s.split(':')
        try:
            h = int(parts[0]); m = int(parts[1])
        except Exception:
            return None
    else:
        if len(s) <= 2:
            try:
                h = int(s); m = 0
            except Exception:
                return None
        else:
            try:
                if len(s) == 3:
                    h = int(s[0]); m = int(s[1:])
                else:
                    h = int(s[:2]); m = int(s[2:])
            except Exception:
                return None
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    return h * 60 + m

def parse_days_fragment(text):
    if not text:
        return None
    t = text.lower().replace('–','-').replace('—','-')
    if re.search(r'weekdays?', t):
        return [0,1,2,3,4]
    if re.search(r'weekends?', t):
        return [5,6]
    if re.search(r'every day|daily|all day|all days', t):
        return list(range(7))
    day_tokens = re.findall(r'(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)', t, re.I)
    if not day_tokens:
        return None
    ranges = re.findall(
        r'(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)'
        r'[\s\-–to]+'
        r'(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)',
        t, re.I
    )
    if ranges:
        for a,b in ranges:
            a_i = DAY_NAME_MAP.get(a[:3].lower())
            b_i = DAY_NAME_MAP.get(b[:3].lower())
            if a_i is not None and b_i is not None:
                if a_i <= b_i:
                    return list(range(a_i, b_i+1))
                else:
                    return list(range(a_i,7)) + list(range(0,b_i+1))
    out = []
    for tok in day_tokens:
        idx = DAY_NAME_MAP.get(tok[:3].lower())
        if idx is not None and idx not in out:
            out.append(idx)
    return sorted(out) if out else None

def parse_price_schedule(text):
    if not text:
        return []
    s = str(text)
    segments = re.split(r'[;\n]|(?<=[0-9])\.\s+', s)
    rules = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        if re.search(r'\b(free|no charge|complimentary|no fee|free of charge)\b', seg, re.I):
            rules.append({
                'days': None, 'start_min': None, 'end_min': None,
                'price_hkd': None, 'unit': 'flat', 'free': True, 'raw': seg
            })
            continue
        days = parse_days_fragment(seg)
        time_match = re.search(
            r'(?P<start>\d{1,2}:\d{2}|\d{3,4}|\d{1,2})\s*[-–to]{1,3}\s*'
            r'(?P<end>\d{1,2}:\d{2}|\d{3,4}|\d{1,2})', seg
        )
        start_min = end_min = None
        if time_match:
            start_min = time_str_to_minutes(time_match.group('start'))
            end_min = time_str_to_minutes(time_match.group('end'))
        m = _money_re.search(seg)
        price_hkd = None
        unit = 'hour' if re.search(r'per hour|/hour|hourly|hr\b', seg, re.I) else 'flat'
        if m:
            amt = m.group('amt').replace(',', '')
            try:
                price_hkd = float(amt)
            except Exception:
                price_hkd = None
        if price_hkd is not None:
            rules.append({
                'days': days,
                'start_min': start_min,
                'end_min': end_min,
                'price_hkd': price_hkd,
                'unit': unit,
                'free': False,
                'raw': seg
            })
    return rules

def price_for_datetime_from_schedule(schedule, dt):
    if not schedule:
        return None
    minutes = dt.hour * 60 + dt.minute
    weekday = dt.weekday()
    for r in schedule:
        days = r.get('days')
        start = r.get('start_min')
        end = r.get('end_min')
        free = r.get('free', False)
        if days is not None and weekday not in days:
            continue
        if start is None and end is None:
            return None if free else r.get('price_hkd')
        if start is not None and end is not None:
            if start <= end:
                if start <= minutes < end:
                    return None if free else r.get('price_hkd')
            else:
                if minutes >= start or minutes < end:
                    return None if free else r.get('price_hkd')
        elif start is not None and end is None:
            if minutes >= start:
                return None if free else r.get('price_hkd')
        elif start is None and end is not None:
            if minutes < end:
                return None if free else r.get('price_hkd')
    return None

def extract_price_from_text_simple(text):
    if not text:
        return None
    t = str(text)
    if re.search(r'\b(free|no charge|complimentary|no fee|free of charge)\b', t, re.I):
        return None
    m = _money_re.search(t)
    if m:
        amt = m.group('amt').replace(',', '')
        try:
            return float(amt)
        except Exception:
            return None
    return None

def fetch_and_cache_info(use_cache=True):
    cache_file = os.path.join(INFO_CACHE_DIR, "carpark_basic_info.json")
    if use_cache and os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    r = get_with_retry(CARPARK_INFO_API, tries=3)
    j = r.json()
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(j, f, ensure_ascii=False)
    return j

def extract_info_columns(rec):
    def get_any(keys):
        for k in keys:
            if k in rec and rec[k] not in (None, ""):
                return rec[k]
        for k in rec.keys():
            if k.lower() in [x.lower() for x in keys]:
                return rec[k]
        return None

    park_id = get_any(["park_id","ParkID","parkId","parkNo","carpark_id","park"])
    name_en = get_any(["name_en","nameEn","name","carpark_name_en","carpark_name"])
    displayAddress_en = get_any(["displayAddress_en","displayAddressEn","displayAddress","address_en","address"])
    district_en = get_any(["district_en","district","districtEn","district_name"])
    contactNo = get_any(["contactNo","contact_no","contact","tel","telephone"])
    opening_status = get_any(["opening_status","openingStatus","status","open_status"])
    latitude = get_any(["latitude","lat","y"])
    longitude = get_any(["longitude","lon","lng","x"])
    if (latitude is None or longitude is None) and isinstance(rec.get("location"), dict):
        loc = rec.get("location")
        latitude = latitude or loc.get("latitude") or loc.get("lat")
        longitude = longitude or loc.get("longitude") or loc.get("lon") or loc.get("lng")
    carpark_photo = get_any(["carpark_photo","photos","photo","carparkPhoto"])
    if isinstance(carpark_photo, list):
        first = carpark_photo[0] if carpark_photo else None
        if isinstance(first, dict):
            carpark_photo = first.get("url") or first.get("photoUrl") or json.dumps(first, ensure_ascii=False)
        else:
            carpark_photo = first
    elif isinstance(carpark_photo, dict):
        carpark_photo = carpark_photo.get("url") or carpark_photo.get("photoUrl") or json.dumps(carpark_photo, ensure_ascii=False)
    remark_en = get_any(["remark_en","remarkEn","remark","remarks","note_en","noteEn"])
    price_amount_hkd = extract_price_from_text_simple(remark_en)
    price_schedule = parse_price_schedule(remark_en) if remark_en else []
    try:
        latitude = float(latitude) if latitude not in (None,"") else None
    except Exception:
        latitude = None
    try:
        longitude = float(longitude) if longitude not in (None,"") else None
    except Exception:
        longitude = None
    if price_amount_hkd is not None:
        if float(price_amount_hkd).is_integer():
            price_str = f"HK${int(price_amount_hkd)}"
        else:
            price_str = f"HK${price_amount_hkd}"
    else:
        price_str = None
    return {
        "park_id": str(park_id) if park_id is not None else None,
        "name_en": name_en,
        "displayAddress_en": displayAddress_en,
        "latitude": latitude,
        "longitude": longitude,
        "district_en": district_en,
        "contactNo": contactNo,
        "opening_status": opening_status,
        "carpark_photo": carpark_photo,
        "price": price_str,
        "price_amount_hkd": price_amount_hkd,
        "price_schedule": json.dumps(price_schedule, ensure_ascii=False),
        "remark_en": remark_en,
        "_raw_info": json.dumps(rec, ensure_ascii=False)
    }

def normalize_info_records(raw):
    recs = []
    if isinstance(raw, list):
        recs = raw
    elif isinstance(raw, dict):
        for k in ("data","records","items","result","carpark_info","carparks"):
            if k in raw and isinstance(raw[k], list):
                recs = raw[k]; break
    if not recs:
        def find_largest(o):
            best=[]
            def _w(x):
                nonlocal best
                if isinstance(x, list):
                    if len(x) > len(best) and all(isinstance(i, dict) for i in x[:10]):
                        best = x
                    for it in x:
                        _w(it)
                elif isinstance(x, dict):
                    for v in x.values():
                        _w(v)
            _w(o); return best
        recs = find_largest(raw)
    rows = []
    for r in recs:
        try:
            rows.append(extract_info_columns(r))
        except Exception:
            continue
    df = pd.DataFrame(rows)
    if "park_id" in df.columns:
        df["park_id"] = df["park_id"].astype(str)
    return df

def _guess_datetime_from_row(row):
    candidates = []
    for c in ('timestamp','datetime','date_time','dateTime','recorded_at','time','date','snapshot_requested_utc'):
        if c in row and pd.notna(row[c]):
            candidates.append((c, row[c]))
    if not candidates:
        for c, v in row.items():
            if isinstance(v, str) and re.search(r'\d{4}-\d{2}-\d{2}', v):
                candidates.append((c, v))
    if not candidates:
        return None
    if 'date' in row and 'time' in row and pd.notna(row.get('date')) and pd.notna(row.get('time')):
        try:
            return pd.to_datetime(f"{row['date']} {row['time']}")
        except Exception:
            pass
    for c, val in candidates:
        try:
            dt = pd.to_datetime(val, infer_datetime_format=True, utc=False, errors='coerce')
            if pd.notna(dt):
                return pd.Timestamp(dt).to_pydatetime()
        except Exception:
            continue
    return None

def build_merged_df(df_vacancy):
    print("Normalizing vacancy `df`...")
    df_flat = df_vacancy.copy()

    if "park_id" not in df_flat.columns:
        for alt in ("park_Id","parkId","carpark_id","park"):
            if alt in df_flat.columns:
                df_flat = df_flat.rename(columns={alt: "park_id"})
                break
    df_flat["park_id"] = df_flat["park_id"].astype(str)

    if DRY_RUN:
        uniq = df_flat["park_id"].dropna().unique()[:DRY_MAX_PARKS]
        df_flat = df_flat[df_flat["park_id"].isin(uniq)].copy()
        print(f"DRY_RUN: limited to {len(uniq)} car parks in merge step")

    print("Fetching basic carpark info...")
    raw_info = fetch_and_cache_info(use_cache=True)
    info_df = normalize_info_records(raw_info)
    print(f"Got info for {len(info_df)} car parks in info_df")

    merged = df_flat.merge(info_df, how="left", on="park_id", suffixes=("","_info"))

    columns_needed = [
        'park_id', 'name_en', 'displayAddress_en', 'latitude', 'longitude',
        'district_en', 'contactNo', 'opening_status', 'carpark_photo', 'price'
    ]
    for col in columns_needed:
        if col not in merged.columns:
            merged[col] = None

    if 'price_schedule' in merged.columns:
        def _compute_price_at_row(row):
            ps_json = row.get('price_schedule')
            if pd.isna(ps_json) or not ps_json:
                return None
            try:
                sched = json.loads(ps_json)
            except Exception:
                return None
            dt = _guess_datetime_from_row(row)
            if dt is None:
                return None
            try:
                return price_for_datetime_from_schedule(sched, dt)
            except Exception:
                return None
        merged['price_at_record_time_hkd'] = merged.apply(_compute_price_at_row, axis=1)
    else:
        merged['price_at_record_time_hkd'] = None

    if 'price_amount_hkd' in merged.columns:
        def _to_float(x):
            try:
                return float(x) if pd.notna(x) else None
            except Exception:
                return None
        merged['price_amount_hkd'] = merged['price_amount_hkd'].apply(_to_float)

    remaining = [c for c in merged.columns if c not in columns_needed]
    merged = merged[columns_needed + remaining]

    merged_df = merged.copy()
    print(f"\nMerged complete! → merged_df has {len(merged_df):,} rows")
    print(f"Car parks with pricing: {merged_df['price_amount_hkd'].notna().sum()}")
    print(f"Rows with time-specific price: {merged_df['price_at_record_time_hkd'].notna().sum()}")

    return merged_df



# upload dataFrame to Google Sheet
def upload_dataframe_to_sheet(df):
    # 從 GitHub Secrets 取得 OAuth token
    token_data = json.loads(os.environ["GCP_TOKEN_JSON"])
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    service = build("sheets", "v4", credentials=creds)

    # 先清空工作表（這裡假設欄位不會超過 Z 欄）
    clear_range = f"{SHEET_NAME}!A:Z"
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=clear_range,
        body={}
    ).execute()

    # 用欄名 + 資料寫入
    values = [list(df.columns)] + df.astype(str).values.tolist()
    body = {"values": values}

    write_range = f"{SHEET_NAME}!A1"
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=write_range,
        valueInputOption="RAW",
        body=body
    ).execute()



# ========================= MAIN =========================
if __name__ == "__main__":
    vacancy_df = build_vacancy_df()
    merged_df = build_merged_df(vacancy_df)
    upload_dataframe_to_sheet(merged_df)
    # 這裡可以加上輸出 CSV 或上傳 Google Sheet 的程式
    # 例如：merged_df.to_csv("merged_df.csv", index=False)
