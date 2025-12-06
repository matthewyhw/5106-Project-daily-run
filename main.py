# Colab-ready: fetch carpark basic info, parse time-aware hourly prices from remark_en,
# only extract prices that have a leading '$' (assume HKD) and provide numeric HKD fields.
import os, time, json, requests, re
import pandas as pd
from pathlib import Path
from datetime import datetime
import math

# CONFIG - change paths as needed
CARPARK_INFO_API = "https://resource.data.one.gov.hk/td/carpark/basic_info_all.json"
FLATTENED_CSV_PATH = "/content/carpark_output/carpark_vacancy_28days_flat.csv"  # adjust if needed
OUTPUT_DIR = "/content/carpark_output_merged"
MERGED_CSV = os.path.join(OUTPUT_DIR, "carpark_vacancy_28days_merged.csv")
PER_DAY_DIR = os.path.join(OUTPUT_DIR, "per_day")
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PER_DAY_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Dry run controls
DRY_RUN = True
DRY_MAX_PARKS = 20
REQUEST_SLEEP = 0.1

# Requested columns (keeps 'price' summary); we'll add numeric HKD columns
columns_needed = [
    'park_id', 'name_en', 'displayAddress_en', 'latitude', 'longitude',
    'district_en', 'contactNo', 'opening_status', 'carpark_photo', 'price'
]

session = requests.Session()
session.headers.update({"User-Agent":"colab-carpark-merge/1.0"})

def get_with_retry(url, params=None, tries=3, timeout=30, backoff=1.0):
    last_exc = None
    for i in range(tries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(backoff*(1+i))
    raise last_exc

def fetch_and_cache_info(use_cache=True):
    cache_file = os.path.join(CACHE_DIR, "carpark_basic_info.json")
    if use_cache and os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    r = get_with_retry(CARPARK_INFO_API, tries=3)
    j = r.json()
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(j, f, ensure_ascii=False)
    return j

# Helpers for parsing
DAY_NAME_MAP = {
    'mon': 0, 'monday': 0,
    'tue': 1, 'tues': 1, 'tuesday': 1,
    'wed': 2, 'wednesday': 2,
    'thu': 3, 'thurs': 3, 'thursday': 3,
    'fri': 4, 'friday': 4,
    'sat': 5, 'saturday': 5,
    'sun': 6, 'sunday': 6
}

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
            except:
                return None
        else:
            try:
                if len(s) == 3:
                    h = int(s[0]); m = int(s[1:])
                else:
                    h = int(s[:2]); m = int(s[2:])
            except:
                return None
    if h < 0 or h > 23 or m < 0 or m > 59:
        return None
    return h*60 + m

# Only match prices that include a leading '$' or 'HK$' -> interpret as HKD
_money_re = re.compile(r'(?P<cur>HK\$|\$)\s*(?P<amt>\d{1,3}(?:,\d{3})*(?:\.\d+)?)', re.I)

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
    ranges = re.findall(r'(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)[\s\-–to]+(mon(?:day)?|tue(?:sday)?|wed(?:nesday)?|thu(?:rsday)?|fri(?:day)?|sat(?:urday)?|sun(?:day)?)', t, re.I)
    if ranges:
        for a,b in ranges:
            a_i = DAY_NAME_MAP.get(a[:3].lower()); b_i = DAY_NAME_MAP.get(b[:3].lower())
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
    """
    Parse free-form remark text and return list of rules with numeric 'price_hkd' only when $ present.
    Each rule: {'days': [...], 'start_min': int or None, 'end_min': int or None, 'price_hkd': float or None, 'unit': 'hour'|'flat', 'raw': segment}
    """
    if not text:
        return []
    s = str(text)
    segments = re.split(r'[;\n]|(?<=[0-9])\.\s+', s)
    rules = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        # detect free -> keep as None price with free=True
        if re.search(r'\b(free|no charge|complimentary|no fee|free of charge)\b', seg, re.I):
            rules.append({'days': None, 'start_min': None, 'end_min': None, 'price_hkd': None, 'unit': 'flat', 'free': True, 'raw': seg})
            continue
        days = parse_days_fragment(seg)
        time_match = re.search(r'(?P<start>\d{1,2}:\d{2}|\d{3,4}|\d{1,2})\s*[-–to]{1,3}\s*(?P<end>\d{1,2}:\d{2}|\d{3,4}|\d{1,2})', seg)
        start_min = end_min = None
        if time_match:
            start_min = time_str_to_minutes(time_match.group('start'))
            end_min = time_str_to_minutes(time_match.group('end'))
        # Require $ in the segment for price extraction
        m = _money_re.search(seg)
        price_hkd = None
        unit = 'hour' if re.search(r'per hour|/hour|hourly|hr\b', seg, re.I) else 'flat'
        if m:
            amt = m.group('amt').replace(',', '')
            try:
                price_hkd = float(amt)
            except:
                price_hkd = None
        # Only add rule if we found a $ price or a free marker
        if price_hkd is not None:
            rules.append({'days': days, 'start_min': start_min, 'end_min': end_min, 'price_hkd': price_hkd, 'unit': unit, 'free': False, 'raw': seg})
        # else skip (do not fallback to numbers without $)
    return rules

def price_for_datetime_from_schedule(schedule, dt):
    if not schedule:
        return None
    minutes = dt.hour*60 + dt.minute
    weekday = dt.weekday()
    for r in schedule:
        days = r.get('days')
        start = r.get('start_min')
        end = r.get('end_min')
        free = r.get('free', False)
        # day match
        if days is not None and weekday not in days:
            continue
        # time match
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
    """
    Only return a price if a leading '$' is present. Return numeric HKD float.
    """
    if not text:
        return None
    t = str(text)
    if re.search(r'\b(free|no charge|complimentary|no fee|free of charge)\b', t, re.I):
        return None  # represent free as None for numeric field; keep human 'Free' in remark if desired
    m = _money_re.search(t)
    if m:
        amt = m.group('amt').replace(',', '')
        try:
            return float(amt)
        except:
            return None
    return None  # Do NOT fallback to numbers without $

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
    # numeric HKD price (only if $ present)
    price_amount_hkd = extract_price_from_text_simple(remark_en)
    # structured schedule (only entries with $ are included)
    price_schedule = parse_price_schedule(remark_en) if remark_en else []

    try:
        latitude = float(latitude) if latitude not in (None,"") else None
    except Exception:
        latitude = None
    try:
        longitude = float(longitude) if longitude not in (None,"") else None
    except Exception:
        longitude = None

    # keep human-readable 'price' column as 'HK$<amt>' if present
    price_str = f"HK${int(price_amount_hkd) if price_amount_hkd is not None and price_amount_hkd.is_integer() else price_amount_hkd}" if price_amount_hkd is not None else None

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
        for k in ("data","records","items","result"):
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
    for c in ('timestamp','datetime','date_time','dateTime','recorded_at','time','date'):
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
        except:
            pass
    for c, val in candidates:
        try:
            dt = pd.to_datetime(val, infer_datetime_format=True, utc=False, errors='coerce')
            if pd.notna(dt):
                return pd.Timestamp(dt).to_pydatetime()
        except:
            continue
    return None

def merge_flattened_with_info(flatten_csv_path, info_df, dry_run=True, dry_limit=20):
    if not Path(flatten_csv_path).exists():
        raise FileNotFoundError(f"Flattened CSV not found: {flatten_csv_path}")
    df_flat = pd.read_csv(flatten_csv_path, dtype=str)
    if "park_id" not in df_flat.columns:
        for alt in ("park_Id","parkId","carpark_id","park"):
            if alt in df_flat.columns:
                df_flat["park_id"] = df_flat[alt].astype(str); break
    if "park_id" not in df_flat.columns:
        raise KeyError("Could not find park id in flattened CSV.")
    if dry_run:
        uniq = df_flat["park_id"].dropna().unique()[:dry_limit]
        df_flat = df_flat[df_flat["park_id"].isin(uniq)].copy()
        print(f"DRY_RUN: keeping {len(uniq)} park_ids")

    if "park_id" not in info_df.columns:
        if "park_Id" in info_df.columns:
            info_df = info_df.rename(columns={"park_Id":"park_id"})
    merged = df_flat.merge(info_df, how="left", on="park_id", suffixes=("","_info"))

    for col in columns_needed:
        if col not in merged.columns:
            merged[col] = None

    # compute price_at_record_time_hkd numeric where possible
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

    # if price_amount_hkd exists from info, propagate to numeric column (ensure float)
    if 'price_amount_hkd' in merged.columns:
        def _to_float(x):
            try:
                return float(x) if pd.notna(x) else None
            except:
                return None
        merged['price_amount_hkd'] = merged['price_amount_hkd'].apply(_to_float)

    remaining = [c for c in merged.columns if c not in columns_needed]
    merged = merged[columns_needed + remaining]
    return merged

# RUN
if __name__ == '__main__':
    print("Fetching basic carpark info (cached)...")
    raw_info = fetch_and_cache_info(use_cache=True)
    info_df = normalize_info_records(raw_info)
    print("Info records:", len(info_df))

    print("Merging with flattened CSV at:", FLATTENED_CSV_PATH)
    merged_df = merge_flattened_with_info(FLATTENED_CSV_PATH, info_df, dry_run=DRY_RUN, dry_limit=DRY_MAX_PARKS)

    print("Merged rows:", len(merged_df))
    merged_df.to_csv(MERGED_CSV, index=False)
    print("Saved merged CSV to:", MERGED_CSV)

    if "date" in merged_df.columns:
        for d, sub in merged_df.groupby(merged_df["date"]):
            safe = str(d).replace(" ", "_")
            outp = os.path.join(PER_DAY_DIR, f"merged_{safe}.csv")
            sub.to_csv(outp, index=False)
        print("Saved per-day CSVs to:", PER_DAY_DIR)
