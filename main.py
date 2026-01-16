import os
import json
import base64
import sys
import time
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus

# Sort is in alpaca.common.enums in alpaca-py (0.4x+). Fallback included for safety.
try:
    from alpaca.common.enums import Sort
except ImportError:  # pragma: no cover
    from alpaca.trading.enums import Sort  # type: ignore

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


LOCAL_TZ_DEFAULT = "America/Denver"
TAB_NAME_DEFAULT = "Orders"


# -------------------------
# Time gating (8am Denver)
# -------------------------
def should_run_now() -> bool:
    """
    Designed for Railway Cron that triggers in UTC.
    We run ONLY when it's ~8:00am in America/Denver (weekdays),
    unless FORCE_RUN=1.
    """
    if os.getenv("FORCE_RUN", "").strip() == "1":
        return True

    tz = ZoneInfo(os.getenv("LOCAL_TZ", LOCAL_TZ_DEFAULT))
    now = dt.datetime.now(tz)

    if now.weekday() >= 5:  # 5=Sat, 6=Sun
        print(f"[skip] Weekend in {tz}: {now.isoformat()}")
        return False

    # Railway cron can drift a few minutes; allow a window.
    window_minutes = int(os.getenv("RUN_WINDOW_MINUTES", "20"))
    if now.hour == 8 and 0 <= now.minute <= window_minutes:
        return True

    print(f"[skip] Not in run window in {tz}: {now.isoformat()}")
    return False


# -------------------------
# Alpaca: fetch all orders
# -------------------------
def _order_to_dict(order_obj: Any) -> Dict[str, Any]:
    # alpaca-py orders are Pydantic models
    if hasattr(order_obj, "model_dump"):
        return order_obj.model_dump()
    if hasattr(order_obj, "dict"):
        return order_obj.dict()
    return dict(order_obj)


def fetch_all_orders(trading: TradingClient, max_pages: int = 20000) -> List[Dict[str, Any]]:
    """
    Fetch ALL orders by paging backwards in time with:
      - status=all
      - limit=500 (max)
      - direction=desc
      - until=<oldest_submitted_at - epsilon>

    Dedup by id as a safety net.
    """
    limit = 500  # GetOrdersRequest max is 500
    until: Optional[dt.datetime] = None
    seen: Dict[str, Dict[str, Any]] = {}

    for page in range(1, max_pages + 1):
        req = GetOrdersRequest(
            status=QueryOrderStatus.ALL,
            limit=limit,
            direction=Sort.DESC,
            until=until,
            nested=False,
        )

        batch = trading.get_orders(filter=req)
        batch_dicts = [_order_to_dict(o) for o in batch]

        print(f"[alpaca] page={page} fetched={len(batch_dicts)} until={until.isoformat() if until else None}")

        if not batch_dicts:
            break

        # Dedup
        for od in batch_dicts:
            oid = str(od.get("id") or "")
            if oid:
                seen[oid] = od

        # Find the oldest submission timestamp in this batch
        def pick_ts(od: Dict[str, Any]) -> Optional[dt.datetime]:
            # alpaca usually provides submitted_at; fall back to created_at
            for k in ("submitted_at", "created_at", "updated_at"):
                v = od.get(k)
                if isinstance(v, dt.datetime):
                    return v
                if isinstance(v, str) and v:
                    try:
                        return dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
                    except Exception:
                        pass
            return None

        ts_list = [pick_ts(od) for od in batch_dicts]
        ts_list = [t for t in ts_list if t is not None]
        if not ts_list:
            print("[alpaca] No timestamps found; stopping pagination.")
            break

        oldest = min(ts_list)

        # Move "until" just before the oldest timestamp to avoid repeats
        until = oldest - dt.timedelta(microseconds=1)

        # If we got less than limit, we're done
        if len(batch_dicts) < limit:
            break

        # Small throttle (optional)
        time.sleep(float(os.getenv("ALPACA_THROTTLE_SECONDS", "0.1")))

    # Return orders sorted oldest->newest for nicer sheet reading
    orders = list(seen.values())

    def sort_key(od: Dict[str, Any]) -> Tuple:
        for k in ("submitted_at", "created_at"):
            v = od.get(k)
            if isinstance(v, dt.datetime):
                return (v,)
            if isinstance(v, str) and v:
                try:
                    return (dt.datetime.fromisoformat(v.replace("Z", "+00:00")),)
                except Exception:
                    pass
        return (dt.datetime.min.replace(tzinfo=dt.timezone.utc),)

    orders.sort(key=sort_key)
    return orders


# -------------------------
# Google Sheets helpers
# -------------------------
def load_service_account_info() -> Dict[str, Any]:
    """
    Accept either:
      - GOOGLE_SERVICE_ACCOUNT_JSON  (raw JSON)
      - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64 JSON)
    """
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

    if b64:
        decoded = base64.b64decode(b64).decode("utf-8")
        return json.loads(decoded)

    if raw:
        return json.loads(raw)

    raise RuntimeError(
        "Missing Google credentials. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_JSON_B64."
    )


def sheets_service():
    info = load_service_account_info()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_sheet_id_map(svc, spreadsheet_id: str) -> Dict[str, int]:
    resp = svc.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties(sheetId,title)"
    ).execute()
    out = {}
    for s in resp.get("sheets", []):
        props = s.get("properties", {})
        title = props.get("title")
        sid = props.get("sheetId")
        if title is not None and sid is not None:
            out[title] = sid
    return out


def ensure_tab_exists(svc, spreadsheet_id: str, tab_name: str) -> None:
    tab_map = get_sheet_id_map(svc, spreadsheet_id)
    if tab_name in tab_map:
        return

    print(f"[sheets] Creating tab: {tab_name}")
    body = {
        "requests": [
            {"addSheet": {"properties": {"title": tab_name}}}
        ]
    }
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def clear_tab_values(svc, spreadsheet_id: str, tab_name: str) -> None:
    print(f"[sheets] Clearing values in tab: {tab_name}")
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=tab_name,
        body={}
    ).execute()


def chunked_write_values(
    svc,
    spreadsheet_id: str,
    tab_name: str,
    values: List[List[Any]],
    chunk_rows: int = 5000,
) -> None:
    """
    Writes values starting at A1 using values.batchUpdate in chunks.
    """
    data = []
    for start in range(0, len(values), chunk_rows):
        chunk = values[start:start + chunk_rows]
        start_row = 1 + start  # 1-indexed
        rng = f"{tab_name}!A{start_row}"
        data.append({"range": rng, "values": chunk})

    body = {"valueInputOption": "RAW", "data": data}
    print(f"[sheets] Writing {len(values)} rows in {len(data)} chunk(s)")
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()


# -------------------------
# Formatting for sheet
# -------------------------
DEFAULT_COLUMNS = [
    "id",
    "client_order_id",
    "symbol",
    "side",
    "type",
    "time_in_force",
    "qty",
    "notional",
    "filled_qty",
    "filled_avg_price",
    "limit_price",
    "stop_price",
    "trail_price",
    "trail_percent",
    "status",
    "created_at",
    "submitted_at",
    "filled_at",
    "canceled_at",
    "expired_at",
    "failed_at",
    "replaced_at",
    "updated_at",
    "asset_class",
    "order_class",
    "extended_hours",
]


def normalize_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return str(v)


def build_sheet_table(orders: List[Dict[str, Any]]) -> List[List[Any]]:
    cols = os.getenv("ORDERS_COLUMNS", "")
    columns = [c.strip() for c in cols.split(",") if c.strip()] if cols else DEFAULT_COLUMNS

    rows: List[List[Any]] = [columns]
    for od in orders:
        rows.append([normalize_cell(od.get(c)) for c in columns])

    return rows


# -------------------------
# Entrypoint
# -------------------------
def main() -> int:
    if not should_run_now():
        return 0

    alpaca_key = os.getenv("ALPACA_API_KEY", "").strip()
    alpaca_secret = os.getenv("ALPACA_API_SECRET", "").strip()
    paper = os.getenv("ALPACA_PAPER", "1").strip() == "1"

    if not alpaca_key or not alpaca_secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_API_SECRET")

    spreadsheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")

    tab_name = os.getenv("GOOGLE_SHEET_TAB_ORDERS", TAB_NAME_DEFAULT).strip() or TAB_NAME_DEFAULT

    print(f"[run] paper={paper} sheet={spreadsheet_id} tab={tab_name}")

    trading = TradingClient(alpaca_key, alpaca_secret, paper=paper)
    orders = fetch_all_orders(trading)
    print(f"[alpaca] total_unique_orders={len(orders)}")

    table = build_sheet_table(orders)

    svc = sheets_service()
    ensure_tab_exists(svc, spreadsheet_id, tab_name)
    clear_tab_values(svc, spreadsheet_id, tab_name)

    chunk_rows = int(os.getenv("SHEETS_WRITE_CHUNK_ROWS", "5000"))
    chunked_write_values(svc, spreadsheet_id, tab_name, table, chunk_rows=chunk_rows)

    print("[done] export complete")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except HttpError as e:
        print(f"[error] Google API error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(1)
