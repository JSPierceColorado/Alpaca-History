import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from dateutil import parser as dtparser

# ----------------------------
# Config (env)
# ----------------------------
SPREADSHEET_ID = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]   # Active-Investing spreadsheet id
WORKSHEET_NAME = os.getenv("HISTORY_SHEET_NAME", "History")

# Alpaca Trading API (paper default)
ALPACA_BASE_URL = os.getenv("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_KEY = os.environ["ALPACA_API_KEY_ID"]
ALPACA_SECRET = os.environ["ALPACA_API_SECRET_KEY"]

# Pull behavior
PAGE_SIZE = int(os.getenv("ALPACA_ACTIVITIES_PAGE_SIZE", "100"))
MAX_PAGES = int(os.getenv("ALPACA_ACTIVITIES_MAX_PAGES", "10"))
STOP_AFTER_CONSECUTIVE_KNOWN = int(os.getenv("ALPACA_STOP_AFTER_CONSEC_KNOWN", "150"))

# Sheet behavior
HEADER_ROW = 1
DATA_START_ROW = 2
MANAGED_PREFIXES = ("alpaca.", "meta.")

# How many columns we will search for/manage (A.. ?). Anything beyond is never touched.
# You can put your personal columns safely to the RIGHT of this range.
RESERVED_MANAGED_COLS = int(os.getenv("HISTORY_RESERVED_MANAGED_COLS", "120"))

# Optional loop mode
RUN_EVERY_SECONDS = int(os.getenv("RUN_EVERY_SECONDS", "0"))  # 0 = run once and exit


# ----------------------------
# Helpers
# ----------------------------
def col_to_a1(col: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA"""
    s = ""
    while col > 0:
        col, r = divmod(col - 1, 26)
        s = chr(65 + r) + s
    return s

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_cell_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float, bool, str)):
        return str(v)
    # lists/dicts -> json
    try:
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return str(v)

def truncate_cell(s: str, limit: int = 45000) -> str:
    # Google Sheets cells have a max size; keep some buffer.
    if s is None:
        return ""
    return s if len(s) <= limit else s[:limit] + "…"

def flatten(obj: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{parent_key}{sep}{k}" if parent_key else str(k)
            if isinstance(v, dict):
                out.update(flatten(v, key, sep=sep))
            elif isinstance(v, list):
                # keep lists as compact JSON
                out[key] = v
            else:
                out[key] = v
    else:
        out[parent_key or "value"] = obj
    return out

def effective_time(activity: Dict[str, Any]) -> str:
    # Based on docs/examples: trade uses transaction_time; NTA often uses date. :contentReference[oaicite:2]{index=2}
    for k in ("transaction_time", "date", "settle_date", "timestamp", "created_at"):
        if k in activity and activity[k]:
            return str(activity[k])
    return ""

def alpaca_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }

def fetch_account_activities() -> List[Dict[str, Any]]:
    """
    Fetch recent activities in descending order with pagination via page_token/page_size. :contentReference[oaicite:3]{index=3}
    """
    url = f"{ALPACA_BASE_URL.rstrip('/')}/v2/account/activities"
    activities: List[Dict[str, Any]] = []

    page_token: Optional[str] = None
    for _ in range(MAX_PAGES):
        params = {
            "direction": "desc",
            "page_size": PAGE_SIZE,
        }
        if page_token:
            params["page_token"] = page_token

        r = requests.get(url, headers=alpaca_headers(), params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()  # list
        if not isinstance(batch, list) or not batch:
            break

        activities.extend(batch)
        page_token = batch[-1].get("id")
        if not page_token:
            break

    return activities


# ----------------------------
# Sheets logic
# ----------------------------
def get_gspread_client() -> gspread.Client:
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_worksheet(gc: gspread.Client) -> gspread.Worksheet:
    sh = gc.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=WORKSHEET_NAME, rows=2000, cols=RESERVED_MANAGED_COLS + 20)

def read_header_row(ws: gspread.Worksheet) -> List[str]:
    end_col = col_to_a1(RESERVED_MANAGED_COLS)
    values = ws.get(f"A{HEADER_ROW}:{end_col}{HEADER_ROW}")
    if not values:
        return [""] * RESERVED_MANAGED_COLS
    row = values[0]
    # pad to full reserved width
    if len(row) < RESERVED_MANAGED_COLS:
        row += [""] * (RESERVED_MANAGED_COLS - len(row))
    return row[:RESERVED_MANAGED_COLS]

def build_managed_header_map(headers: List[str]) -> Dict[str, int]:
    # header -> 1-based column index
    out: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        if isinstance(h, str) and h and h.startswith(MANAGED_PREFIXES):
            out[h] = i
    return out

def first_empty_header_col(headers: List[str]) -> Optional[int]:
    # empty = "" in reserved range
    for i, h in enumerate(headers, start=1):
        if not h:
            return i
    return None

def ensure_base_headers(ws: gspread.Worksheet) -> Tuple[List[str], Dict[str, int]]:
    headers = read_header_row(ws)
    hmap = build_managed_header_map(headers)

    base = [
        "meta.id",
        "meta.effective_time",
        "meta.pulled_at_utc",
        "meta.raw_json",
        "alpaca.activity_type",
    ]

    updates = []
    for name in base:
        if name in hmap:
            continue
        empty_col = first_empty_header_col(headers)
        if empty_col is None:
            raise RuntimeError(
                f"No empty header columns left inside reserved managed range (1..{RESERVED_MANAGED_COLS}). "
                f"Increase HISTORY_RESERVED_MANAGED_COLS."
            )
        headers[empty_col - 1] = name
        updates.append((empty_col, name))
        hmap[name] = empty_col

    # write any new base headers
    if updates:
        for col, name in updates:
            ws.update_cell(HEADER_ROW, col, name)

    return headers, hmap

def claim_header(ws: gspread.Worksheet, headers: List[str], hmap: Dict[str, int], name: str) -> int:
    if name in hmap:
        return hmap[name]
    empty_col = first_empty_header_col(headers)
    if empty_col is None:
        # fall back: don't create more columns; caller should rely on meta.raw_json
        return -1
    headers[empty_col - 1] = name
    ws.update_cell(HEADER_ROW, empty_col, name)
    hmap[name] = empty_col
    return empty_col

def get_existing_id_rows(ws: gspread.Worksheet, id_col: int) -> Dict[str, int]:
    # col_values returns up to last non-empty; row 1 is header
    col_vals = ws.col_values(id_col)
    out: Dict[str, int] = {}
    for idx, v in enumerate(col_vals[1:], start=2):
        if v:
            out[v] = idx
    return out

def insert_empty_rows(ws: gspread.Worksheet, how_many: int, at_row: int) -> None:
    if how_many <= 0:
        return
    # Use Sheets API insertDimension to insert completely empty rows.
    sheet_id = ws.id
    # 0-based indices; startIndex=at_row-1
    start = at_row - 1
    body = {
        "requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": start,
                    "endIndex": start + how_many
                },
                "inheritFromBefore": False
            }
        }]
    }
    ws.spreadsheet.batch_update(body)

def batch_update_cells(ws: gspread.Worksheet, updates: List[Tuple[int, int, str]]) -> None:
    """
    updates: list of (row, col, value)
    Writes only specific cells (never touches other columns).
    """
    if not updates:
        return

    # group by row for fewer API calls, but still only touch target columns
    updates_by_row: Dict[int, List[Tuple[int, str]]] = {}
    for r, c, v in updates:
        updates_by_row.setdefault(r, []).append((c, v))

    data = []
    for r, cols in updates_by_row.items():
        cols_sorted = sorted(cols, key=lambda x: x[0])
        for c, v in cols_sorted:
            a1 = f"{col_to_a1(c)}{r}"
            data.append({"range": a1, "values": [[v]]})

    ws.batch_update(data, value_input_option="RAW")

def sync_once() -> None:
    gc = get_gspread_client()
    ws = open_worksheet(gc)

    headers, hmap = ensure_base_headers(ws)
    id_col = hmap["meta.id"]

    existing = get_existing_id_rows(ws, id_col)

    activities = fetch_account_activities()

    consecutive_known = 0
    new_acts: List[Dict[str, Any]] = []
    recent_for_update: List[Dict[str, Any]] = []

    for act in activities:
        act_id = str(act.get("id", ""))
        if not act_id:
            continue

        if act_id in existing:
            consecutive_known += 1
            recent_for_update.append(act)
        else:
            consecutive_known = 0
            new_acts.append(act)

        if consecutive_known >= STOP_AFTER_CONSECUTIVE_KNOWN:
            break

    # Insert new rows (newest should end up closest to top, i.e., row 2)
    # We insert N empty rows at row 2, then write values:
    # - row 2 gets newest, row 3 gets next newest, etc.
    new_acts_sorted = sorted(
        new_acts,
        key=lambda a: dtparser.parse(effective_time(a)) if effective_time(a) else datetime.min.replace(tzinfo=timezone.utc),
        reverse=True
    )

    n_new = len(new_acts_sorted)
    if n_new:
        insert_empty_rows(ws, n_new, DATA_START_ROW)

        # Existing rows moved down by n_new
        existing = {k: (r + n_new if r >= DATA_START_ROW else r) for k, r in existing.items()}

    cell_updates: List[Tuple[int, int, str]] = []
    pulled_at = now_iso_utc()

    def stage_activity_write(row: int, act: Dict[str, Any]) -> None:
        nonlocal headers, hmap, cell_updates

        act_id = str(act.get("id", ""))
        act_effective = effective_time(act)
        raw = truncate_cell(json.dumps(act, ensure_ascii=False))

        # base/meta fields
        record: Dict[str, Any] = {
            "meta.id": act_id,
            "meta.effective_time": act_effective,
            "meta.pulled_at_utc": pulled_at,
            "meta.raw_json": raw,
        }

        # Flatten all Alpaca fields into alpaca.<path>
        flat = flatten(act)
        for k, v in flat.items():
            record[f"alpaca.{k}"] = v

        # ensure headers exist for keys we want in columns
        for key in list(record.keys()):
            col = claim_header(ws, headers, hmap, key)
            if col == -1:
                # no space left; rely on meta.raw_json for completeness
                continue
            cell_updates.append((row, col, safe_cell_value(record[key])))

    # Write new ones
    for i, act in enumerate(new_acts_sorted):
        row = DATA_START_ROW + i
        stage_activity_write(row, act)
        existing[str(act.get("id", ""))] = row

    # Update already-existing rows we re-fetched (keeps data “complete” if Alpaca adds fields)
    for act in recent_for_update:
        act_id = str(act.get("id", ""))
        row = existing.get(act_id)
        if row:
            stage_activity_write(row, act)

    batch_update_cells(ws, cell_updates)

    print(f"History sync complete. Inserted {n_new} new activities. Updated {len(recent_for_update)} existing rows.")

def main() -> None:
    while True:
        sync_once()
        if RUN_EVERY_SECONDS <= 0:
            break
        time.sleep(RUN_EVERY_SECONDS)

if __name__ == "__main__":
    main()
