import time
import json
import calendar
import requests
import pandas as pd
from datetime import datetime, timedelta, date

# -------------------- Config --------------------
BASE_URL = "https://resource.data.one.gov.hk/td/carpark/vacancy_all.json"
YEAR = 2024
TARGET_MONTH = 11          # 1..12 for a specific month, or 0 for whole year
INTERVAL_MINUTES = 60      # 1 hour
DRY_RUN = False             # Set to False for full run
LIMIT_SNAPSHOTS = 48       # only used when DRY_RUN=True

# HKT handling (fixed offset, no DST)
HKT_OFFSET = timedelta(hours=8)

# -------------------- Time generation (HKT → UTC) --------------------
def month_start_end_hkt(year: int, month: int):
    days = calendar.monthrange(year, month)[1]
    start = datetime(year, month, 1, 0, 0, 0)
    end = datetime(year, month, days, 23, 0, 0)  # last day 23:00 HKT inclusive
    return start, end


def generate_hourly_hkt_range(year: int, target_month: int | None = None, interval_minutes: int = 60):
    """
    Yield UTC datetimes corresponding to HKT-local scheduled timestamps.
    If target_month is None/0: full year. Else: only that month.
    """
    ranges = []
    if target_month and 1 <= target_month <= 12:
        s_hkt, e_hkt = month_start_end_hkt(year, target_month)
        ranges.append((s_hkt, e_hkt))
    else:
        for m in range(1, 13):
            s_hkt, e_hkt = month_start_end_hkt(year, m)
            ranges.append((s_hkt, e_hkt))

    # For each range, step from start to end inclusive at interval
    for s_hkt, e_hkt in ranges:
        cur = s_hkt
        while cur <= e_hkt:
            yield cur - HKT_OFFSET  # convert to UTC (naive)
            cur += timedelta(minutes=interval_minutes)


def get_timestamps(
    year=YEAR,
    month=TARGET_MONTH,
    interval_minutes=INTERVAL_MINUTES,
    dry_run=DRY_RUN,
    limit=LIMIT_SNAPSHOTS,
):
    all_ts = list(generate_hourly_hkt_range(year, month if month else None, interval_minutes))
    if dry_run:
        return all_ts[-limit:]
    return all_ts


def dry_run_preview(
    year=YEAR,
    month=TARGET_MONTH,
    interval_minutes=INTERVAL_MINUTES,
    limit=LIMIT_SNAPSHOTS,
):
    ts = get_timestamps(year, month, interval_minutes, dry_run=True, limit=limit)
    print(f"Dry run: returning {len(ts)} timestamps (last {limit} samples)")
    if ts:
        first_hkt = ts[0] + HKT_OFFSET
        last_hkt = ts[-1] + HKT_OFFSET
        print(
            "Sample HKT start:",
            first_hkt.strftime("%Y-%m-%d %H:%M"),
            "... end:",
            last_hkt.strftime("%Y-%m-%d %H:%M"),
        )
    return ts

# -------------------- Networking helpers --------------------
session = requests.Session()
session.headers.update({"User-Agent": "python-carpark-collector/1.0"})

def get_with_retry(url, params=None, timeout=30, tries=3, backoff=1.0):
    last_exc = None
    for attempt in range(tries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(backoff * (1 + attempt))
    raise last_exc


def try_direct_api(ts: datetime) -> str | None:
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
                r = get_with_retry(BASE_URL, params={p: s}, tries=2)
                if (
                    r
                    and r.text
                    and (
                        r.text.strip().startswith("{")
                        or r.text.strip().startswith("[")
                        or "application/json" in (r.headers.get("content-type") or "")
                    )
                ):
                    return r.text
            except Exception:
                pass
            time.sleep(0.05)
    # last-try: live endpoint without timestamp
    try:
        r = get_with_retry(BASE_URL, tries=2)
        if r and r.text:
            return r.text
    except Exception:
        pass
    return None


def try_wayback(ts: datetime) -> str | None:
    try:
        ts_str = ts.strftime("%Y%m%d%H%M%S")
        r = get_with_retry(
            "http://archive.org/wayback/available",
            params={"url": BASE_URL, "timestamp": ts_str},
            tries=2,
        )
        j = r.json()
        snap = j.get("archived_snapshots", {}).get("closest")
        if not snap:
            return None
        snap_url = snap.get("url")
        r2 = get_with_retry(snap_url, tries=2)
        txt = r2.text
        first = min([i for i in (txt.find("{"), txt.find("[")) if i != -1] + [None])
        if first is None:
            return txt
        return txt[first:]
    except Exception:
        return None


def fetch_snapshot(ts: datetime) -> str | None:
    """
    無快取版本：每次都直接打 API / Wayback，只保留在記憶體。
    """
    txt = try_direct_api(ts)
    if not txt:
        txt = try_wayback(ts)
    return txt

# -------------------- Flattening --------------------
def flatten_carpark_record(rec: dict, snapshot_ts: datetime):
    rows = []

    park_id = None
    for k in ("park_id", "ParkID", "CarParkID", "carpark_id", "carpark_no", "carpark"):
        if k in rec:
            park_id = rec.get(k)
            break
    if park_id is None:
        for k in rec.keys():
            if "park" in k.lower() or "id" in k.lower():
                park_id = rec.get(k)
                break

    vehicle_types = (
        rec.get("vehicle_type")
        or rec.get("vehicleType")
        or rec.get("vehicle_types")
        or []
    )
    if not isinstance(vehicle_types, list):
        vehicle_types = [vehicle_types]

    if not vehicle_types:
        rows.append(
            {
                "snapshot_requested_utc": snapshot_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "park_id": park_id,
                "vehicle_type": None,
                "service_category": None,
                "vacancy_type": None,
                "vacancy": None,
                "lastupdate": None,
            }
        )
        return rows

    for vt in vehicle_types:
        vt_type = vt.get("type") if isinstance(vt, dict) else str(vt)
        service_cats = vt.get("service_category") if isinstance(vt, dict) else []
        if service_cats is None:
            service_cats = []
        if not isinstance(service_cats, list):
            service_cats = [service_cats]

        if not service_cats:
            rows.append(
                {
                    "snapshot_requested_utc": snapshot_ts.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "park_id": park_id,
                    "vehicle_type": vt_type,
                    "service_category": None,
                    "vacancy_type": None,
                    "vacancy": None,
                    "lastupdate": None,
                }
            )
            continue

        for sc in service_cats:
            if not isinstance(sc, dict):
                rows.append(
                    {
                        "snapshot_requested_utc": snapshot_ts.strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                        "park_id": park_id,
                        "vehicle_type": vt_type,
                        "service_category": str(sc),
                        "vacancy_type": None,
                        "vacancy": None,
                        "lastupdate": None,
                    }
                )
                continue
            category = (
                sc.get("category")
                or sc.get("service")
                or sc.get("service_category")
                or sc.get("type")
            )
            vacancy_type = (
                sc.get("vacancy_type")
                or sc.get("vacancyType")
                or sc.get("vacancy_type_code")
            )
            vacancy_raw = sc.get("vacancy")
            lastupdate = (
                sc.get("lastupdate")
                or sc.get("last_update")
                or sc.get("lastUpdate")
            )
            vacancy = None
            try:
                if vacancy_raw is not None and str(vacancy_raw).strip() != "":
                    vacancy = int(vacancy_raw)
            except Exception:
                vacancy = None
            rows.append(
                {
                    "snapshot_requested_utc": snapshot_ts.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "park_id": park_id,
                    "vehicle_type": vt_type,
                    "service_category": category,
                    "vacancy_type": vacancy_type,
                    "vacancy": vacancy,
                    "lastupdate": lastupdate,
                }
            )
    return rows

# -------------------- Main --------------------
timestamps = get_timestamps(YEAR, TARGET_MONTH, INTERVAL_MINUTES, DRY_RUN, LIMIT_SNAPSHOTS)
print(f"Timestamps to fetch: {len(timestamps)} (DRY_RUN={DRY_RUN})")

all_rows = []
failed = []

for ts in timestamps:
    txt = fetch_snapshot(ts)
    if not txt:
        failed.append(ts)
        time.sleep(0.05)
        continue

    # parse JSON
    parsed = None
    try:
        parsed = json.loads(txt)
    except Exception:
        s = txt
        idxs = [i for i in (s.find("{"), s.find("[")) if i != -1]
        if idxs:
            try:
                parsed = json.loads(s[min(idxs):])
            except Exception:
                parsed = None
    if parsed is None:
        failed.append(ts)
        time.sleep(0.05)
        continue

    # find records list
    recs = []
    if isinstance(parsed, list):
        recs = parsed
    elif isinstance(parsed, dict):
        for candidate in ("data", "records", "items", "result"):
            if candidate in parsed and isinstance(parsed[candidate], list):
                recs = parsed[candidate]
                break
        if not recs:

            def find_largest_list(o):
                best = []

                def _w(x):
                    nonlocal best
                    if isinstance(x, list):
                        if len(x) > len(best) and all(
                            isinstance(i, dict) for i in x[:10]
                        ):
                            best = x
                        for it in x:
                            _w(it)
                    elif isinstance(x, dict):
                        for v in x.values():
                            _w(v)

                _w(o)
                return best

            recs = find_largest_list(parsed)

    if not recs:
        failed.append(ts)
        time.sleep(0.02)
        continue

    for r in recs:
        try:
            rows = flatten_carpark_record(r, ts)
            all_rows.extend(rows)
        except Exception:
            continue

# Build final DataFrame `df`（不輸出 CSV、不寫檔）
df = pd.DataFrame(all_rows)

if df.empty:
    print("No rows collected. Failed snapshots:", len(failed))
else:
    df["snapshot_requested_utc"] = pd.to_datetime(df["snapshot_requested_utc"])
    df["snapshot_requested_hkt"] = df["snapshot_requested_utc"] + HKT_OFFSET
    df["date_hkt"] = df["snapshot_requested_hkt"].dt.date
    df["time_hkt"] = df["snapshot_requested_hkt"].dt.strftime("%H:%M:%S")
    df["year"] = YEAR
    df["month"] = TARGET_MONTH if TARGET_MONTH else None

    print("Rows:", len(df), "| Unique HKT dates:", df["date_hkt"].nunique())

print("Failed snapshots:", len(failed))
if failed and len(failed) <= 20:
    print([t.strftime("%Y-%m-%dT%H:%MZ") for t in failed])


# ------------------- Part two - Merging with car park basic info and public holiday ------------

base_df = df.copy()

# -------------------- 取得停車場基本資料 --------------------
BASIC_INFO_URL = "https://resource.data.one.gov.hk/td/carpark/basic_info_all.json"
session = requests.Session()
session.headers.update({"User-Agent": "python-carpark-enricher/1.4"})

def fetch_basic_info():
    r = session.get(BASIC_INFO_URL, timeout=30)
    r.raise_for_status()
    txt = r.text
    try:
        data = json.loads(txt)
    except Exception:
        idxs = [i for i in (txt.find("{"), txt.find("[")) if i != -1]
        if idxs:
            data = json.loads(txt[min(idxs):])
        else:
            raise
    if isinstance(data, list):
        recs = data
    elif isinstance(data, dict):
        recs = []
        for k in ("data", "records", "items", "result"):
            if k in data and isinstance(data[k], list):
                recs = data[k]
                break
        if not recs:
            def find_largest_list(o):
                best = []
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
                _w(o)
                return best
            recs = find_largest_list(data)
    else:
        recs = []
    return recs

def normalize_basic_info(recs):
    rows = []
    for r in recs:
        park_id = r.get('park_id') or r.get('ParkID') or r.get('CarParkID') or r.get('carpark_id')
        name_en = r.get('name_en') or r.get('nameEn') or r.get('nameEN') or r.get('name')
        displayAddress_en = (
            r.get('displayAddress_en') or r.get('displayAddressEn')
            or r.get('address_en') or r.get('Address_en')
        )
        latitude = r.get('latitude') or r.get('lat') or r.get('Latitude')
        longitude = r.get('longitude') or r.get('lon') or r.get('lng') or r.get('Longitude')
        district_en = r.get('district_en') or r.get('districtEn') or r.get('district')
        opening_status = r.get('opening_status') or r.get('openingStatus') or r.get('status')
        rows.append({
            'park_id': park_id,
            'name_en': name_en,
            'displayAddress_en': displayAddress_en,
            'latitude': latitude,
            'longitude': longitude,
            'district_en': district_en,
            'opening_status': opening_status,
        })
    return pd.DataFrame(rows)

# -------------------- 處理日期欄位（從 df 本身推導） --------------------
if 'snapshot_requested_hkt' in base_df.columns:
    base_df['snapshot_requested_hkt'] = pd.to_datetime(base_df['snapshot_requested_hkt'], errors='coerce')
    base_df['date_hkt_dt'] = base_df['snapshot_requested_hkt']
    # 若原 df 的 date_hkt 是 YYYY-MM-DD，可保留也可覆蓋成 DD/MM/YYYY，看你原邏輯
    base_df['date_hkt'] = base_df['snapshot_requested_hkt'].dt.strftime('%d/%m/%Y')
    if 'time_hkt' not in base_df.columns:
        base_df['time_hkt'] = base_df['snapshot_requested_hkt'].dt.strftime('%H:%M:%S')
elif 'snapshot_requested_utc' in base_df.columns:
    base_df['snapshot_requested_utc'] = pd.to_datetime(base_df['snapshot_requested_utc'], errors='coerce')
    base_df['snapshot_requested_hkt'] = base_df['snapshot_requested_utc'] + pd.Timedelta(hours=8)
    base_df['date_hkt_dt'] = base_df['snapshot_requested_hkt']
    base_df['date_hkt'] = base_df['snapshot_requested_hkt'].dt.strftime('%d/%m/%Y')
    base_df['time_hkt'] = base_df['snapshot_requested_hkt'].dt.strftime('%H:%M:%S')
else:
    # 只剩 date_hkt（假設是 DD/MM/YYYY）
    base_df['date_hkt_dt'] = pd.to_datetime(base_df['date_hkt'], dayfirst=True, errors='coerce')
    base_df['date_hkt'] = base_df['date_hkt_dt'].dt.strftime('%d/%m/%Y')
    if 'time_hkt' not in base_df.columns:
        base_df['time_hkt'] = None

# 週次與星期幾
if 'snapshot_requested_hkt' in base_df.columns:
    base_df['week'] = base_df['snapshot_requested_hkt'].dt.isocalendar().week
    base_df['Days of the week'] = base_df['snapshot_requested_hkt'].dt.day_name()
else:
    base_df['week'] = base_df['date_hkt_dt'].dt.isocalendar().week
    base_df['Days of the week'] = base_df['date_hkt_dt'].dt.day_name()

# -------------------- 合併 basic info --------------------
info_recs = fetch_basic_info()
info_df = normalize_basic_info(info_recs)
merged = base_df.merge(info_df, on='park_id', how='left')

# -------------------- Merge with Public holiday data --------------------
HOLIDAY_URL = "https://www.1823.gov.hk/common/ical/en.json"

DATE_FORMATS = [
    "%Y%m%d",
    "%Y-%m-%d",
    "%Y%m%dT%H%M%S",
    "%Y-%m-%dT%H:%M:%S",
]

def to_date(val):
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return date(val.year, val.month, val.day)
    if isinstance(val, (int, float)):
        val = str(int(val))
    if not isinstance(val, str):
        return None
    s = val.strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return date(dt.year, dt.month, dt.day)
        except Exception:
            continue
    tok = s.replace("T", " ").split()[0]
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(tok, fmt)
            return date(dt.year, dt.month, dt.day)
        except Exception:
            pass
    return None

def parse_value_field(field):
    if field is None:
        return None
    # Array case: ["YYYYMMDD", {"value":"DATE"}]
    if isinstance(field, list):
        if field and isinstance(field[0], str):
            return field[0]
        for v in field:
            if isinstance(v, str):
                return v
        for v in field:
            if isinstance(v, dict) and isinstance(v.get("value"), str):
                return v["value"]
        return None
    if isinstance(field, dict):
        for k in ("value", "date", "start"):
            if isinstance(field.get(k), str):
                return field.get(k)
        for v in field.values():
            if isinstance(v, str):
                return v
        return None
    return field

def fetch_holidays():
    # 直接打 1823 iCal JSON，不再讀寫 CSV
    ddmmyyyy_map = {}  # 這版不再用 DD/MM/YYYY CSV map，保留空 dict
    set_ymd = set(); map_ymd = {}
    set_yyyymmdd = set(); map_yyyymmdd = {}

    r = session.get(HOLIDAY_URL, timeout=30)
    r.raise_for_status()
    feed = r.json()
    vevents = []
    if isinstance(feed, dict):
        vcals = feed.get("vcalendar")
        if isinstance(vcals, list) and vcals:
            vc0 = vcals[0]
            if isinstance(vc0, dict) and "vevent" in vc0:
                vevents = vc0.get("vevent") or []

    for ev in vevents:
        if not isinstance(ev, dict):
            continue
        name = ev.get("summary") or ev.get("name") or ev.get("title") or ""
        d1 = to_date(parse_value_field(ev.get("dtstart")))
        d2 = to_date(parse_value_field(ev.get("dtend"))) or d1
        if d1 and d2 and d2 < d1:
            d1, d2 = d2, d1
        cur = d1
        while cur and cur <= d2:
            ymd = cur.strftime("%Y-%m-%d")
            yyyymmdd = cur.strftime("%Y%m%d")
            set_ymd.add(ymd)
            set_yyyymmdd.add(yyyymmdd)
            nm = str(name).strip()
            map_ymd[ymd] = nm
            map_yyyymmdd[yyyymmdd] = nm
            cur += timedelta(days=1)
    return (ddmmyyyy_map, set_ymd, map_ymd, set_yyyymmdd, map_yyyymmdd)

ddmmyyyy_map, holiday_set_ymd, holiday_map_ymd, holiday_set_yyyymmdd, holiday_map_yyyymmdd = fetch_holidays()

# 構造各種日期 key
merged['date_hkt_dt'] = pd.to_datetime(merged['date_hkt_dt'], errors='coerce')
merged['date_hkt_str_ddmmyyyy'] = merged['date_hkt']                      # 例如 "31/12/2024"
merged['date_hkt_str_ymd'] = merged['date_hkt_dt'].dt.strftime('%Y-%m-%d')
merged['date_hkt_str_yyyymmdd'] = merged['date_hkt_dt'].dt.strftime('%Y%m%d')

# 1) 這版沒有 holiday CSV，因此 ddmmyyyy_map 為空，先全部 False / ""
merged['is_holiday'] = False
merged['holiday_name'] = ""

# 2) 用 YYYYMMDD 對應 1823 JSON
mask = merged['date_hkt_str_yyyymmdd'].notna()
merged.loc[mask, 'is_holiday'] = merged.loc[mask, 'date_hkt_str_yyyymmdd'].isin(holiday_set_yyyymmdd)
merged.loc[mask, 'holiday_name'] = merged.loc[mask, 'date_hkt_str_yyyymmdd'].map(holiday_map_yyyymmdd).fillna(merged.loc[mask, 'holiday_name'])

# 3) 再用 YYYY-MM-DD 補充
mask2 = merged['holiday_name'].eq("")
merged.loc[mask2, 'is_holiday'] = merged.loc[mask2, 'date_hkt_str_ymd'].isin(holiday_set_ymd)
merged.loc[mask2, 'holiday_name'] = merged.loc[mask2, 'date_hkt_str_ymd'].map(holiday_map_ymd).fillna(merged.loc[mask2, 'holiday_name'])

# -------------------- 選取欄位，產出 df_enriched（不寫任何檔案） --------------------
requested_cols = [
    'date_hkt',
    'time_hkt',
    'Days of the week',
    'park_id',
    'name_en',
    'displayAddress_en',
    'district_en',
    'latitude',
    'longitude',
    'vehicle_type',
    'service_category',
    'vacancy_type',
    'vacancy',
    'opening_status',
    'is_holiday',
    'holiday_name',
]
for c in requested_cols:
    if c not in merged.columns:
        merged[c] = None

df_enriched = merged[requested_cols].copy()

# upload dataFrame to Google Sheet
def upload_dataframe_to_sheet(df):
    # 從 GitHub Secrets 取得 OAuth token
    token_data = json.loads(os.environ["GCP_TOKEN_JSON"])
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    service = build("sheets", "v4", credentials=creds)

    # Clear the worksheet (Column no more than ZZ)
    clear_range = f"{SHEET_NAME}!A:ZZ"
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


upload_dataframe_to_sheet(df_enriched)
