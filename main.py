import os
import json
import base64
import re
import sys
import time
import datetime as dt
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


TAB_NAME_DEFAULT = "Orders"


# -------------------------
# Helpers
# -------------------------
def colnum_to_letter(n: int) -> str:
    """
    1 -> A, 26 -> Z, 27 -> AA, ...
    """
    if n <= 0:
        raise ValueError("Column count must be >= 1")
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


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
    limit = 500
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

        # Find the oldest timestamp in this batch to page further back
        def pick_ts(od: Dict[str, Any]) -> Optional[dt.datetime]:
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
        until = oldest - dt.timedelta(microseconds=1)

        if len(batch_dicts) < limit:
            break

        time.sleep(float(os.getenv("ALPACA_THROTTLE_SECONDS", "0.1")))

    # Sort oldest->newest for nicer sheet reading
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
    Preferred:
      - GOOGLE_SERVICE_ACCOUNT_JSON (raw JSON)

    Optional fallback:
      - GOOGLE_SERVICE_ACCOUNT_JSON_B64 (base64 JSON)
        (handles whitespace/newlines + missing padding)
    """
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw:
        return json.loads(raw)

    b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
    if not b64:
        raise RuntimeError(
            "Missing Google credentials. Set GOOGLE_SERVICE_ACCOUNT_JSON (preferred) "
            "or GOOGLE_SERVICE_ACCOUNT_JSON_B64."
        )

    if b64.lstrip().startswith("{"):
        return json.loads(b64)

    cleaned = re.sub(r"\s+", "", b64)
    rem = len(cleaned) % 4
    if rem in (2, 3):
        cleaned += "=" * (4 - rem)
    elif rem == 1:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON_B64 looks corrupted (length {len(cleaned)} mod 4 == 1). "
            "Re-generate it or use GOOGLE_SERVICE_ACCOUNT_JSON instead."
        )

    decoded = base64.b64decode(cleaned)
    return json.loads(decoded.decode("utf-8"))


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
    body = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def clear_export_range_only(svc, spreadsheet_id: str, tab_name: str, last_col_letter: str) -> None:
    """
    Clear ONLY the export columns so we don't touch AA+ (or anything beyond last_col_letter).
    Example: last_col_letter='Z' => clears A:Z.
    """
    rng = f"{tab_name}!A:{last_col_letter}"
    print(f"[sheets] Clearing export range only: {rng}")
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=rng,
        body={}
    ).execute()


def chunked_write_values(
    svc,
    spreadsheet_id: str,
    tab_name: str,
    values: List[List[Any]],
    last_col_letter: str,
    chunk_rows: int = 5000,
) -> None:
    """
    Writes values starting at A1 in chunks.
    We specify explicit ranges (A..last_col_letter) to avoid any accidental spillover.
    """
    data = []
    width = len(values[0]) if values else 1

    for start in range(0, len(values), chunk_rows):
        chunk = values[start:start + chunk_rows]
        start_row = 1 + start  # 1-indexed
        end_row = start_row + len(chunk) - 1
        rng = f"{tab_name}!A{start_row}:{last_col_letter}{end_row}"
        data.append({"range": rng, "values": chunk})

    body = {"valueInputOption": "RAW", "data": data}
    print(f"[sheets] Writing {len(values)} rows in {len(data)} chunk(s) (A..{last_col_letter})")
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


def get_columns() -> List[str]:
    cols = os.getenv("ORDERS_COLUMNS", "")
    if cols.strip():
        return [c.strip() for c in cols.split(",") if c.strip()]
    return DEFAULT_COLUMNS


def build_sheet_table(orders: List[Dict[str, Any]], columns: List[str]) -> List[List[Any]]:
    rows: List[List[Any]] = [columns]
    for od in orders:
        rows.append([normalize_cell(od.get(c)) for c in columns])
    return rows


# -------------------------
# Entrypoint
# -------------------------
def main() -> int:
    alpaca_key = os.getenv("ALPACA_API_KEY", "").strip()
    alpaca_secret = os.getenv("ALPACA_API_SECRET", "").strip()
    paper = os.getenv("ALPACA_PAPER", "1").strip() == "1"

    if not alpaca_key or not alpaca_secret:
        raise RuntimeError("Missing ALPACA_API_KEY / ALPACA_API_SECRET")

    spreadsheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")

    tab_name = os.getenv("GOOGLE_SHEET_TAB_ORDERS", TAB_NAME_DEFAULT).strip() or TAB_NAME_DEFAULT

    columns = get_columns()
    last_col_letter = colnum_to_letter(len(columns))  # default columns -> 26 -> Z

    print(
        f"[run] exporting now | paper={paper} | sheet={spreadsheet_id} | tab={tab_name} "
        f"| cols={len(columns)} (A..{last_col_letter})"
    )

    trading = TradingClient(alpaca_key, alpaca_secret, paper=paper)
    orders = fetch_all_orders(trading)
    print(f"[alpaca] total_unique_orders={len(orders)}")

    table = build_sheet_table(orders, columns)

    svc = sheets_service()
    ensure_tab_exists(svc, spreadsheet_id, tab_name)

    # IMPORTANT: Only clear the export columns (e.g., A:Z) so AA+ remains untouched.
    clear_export_range_only(svc, spreadsheet_id, tab_name, last_col_letter)

    chunk_rows = int(os.getenv("SHEETS_WRITE_CHUNK_ROWS", "5000"))
    chunked_write_values(
        svc,
        spreadsheet_id,
        tab_name,
        table,
        last_col_letter=last_col_letter,
        chunk_rows=chunk_rows,
    )

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
