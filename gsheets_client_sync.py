import os
import re
import json
import time
import random
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional, Iterable, Callable

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from dotenv import load_dotenv

try:
    import requests
    from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout
except Exception:
    requests = None
    RequestsConnectionError = Exception
    RequestsTimeout = Exception

try:
    import google.auth.exceptions as google_auth_exceptions
except Exception:
    class _Dummy(Exception): ...
    google_auth_exceptions = _Dummy

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

def get_kst_tz(name: str = "Asia/Seoul"):
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone(timedelta(hours=9))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

def ENV(k: str, d: Optional[str] = None) -> str:
    return os.getenv(k, d)

GOOGLE_CREDENTIALS_FILENAME = ENV('GOOGLE_CREDENTIALS_FILENAME', 'credentials.json')

POLL_INTERVAL     = int(ENV('POLL_INTERVAL', '180'))
TIMEZONE_NAME     = ENV('TIMEZONE', 'Asia/Seoul')
KST               = get_kst_tz(TIMEZONE_NAME)
SYNC_STATE_FILE   = ENV('SYNC_STATE_FILE', 'client_sync_state.json')
CURSOR_STATE_FILE = ENV('CURSOR_STATE_FILE', 'cursor_state.json')
DUP_WINDOW_DAYS   = int(ENV('DUP_WINDOW_DAYS', '30'))

SRC_DOC_URL  = ENV('SRC_DOC_URL')
SRC_TAB      = ENV('SRC_TAB', '시트1')
SRC_COL_DATE = ENV('SRC_COL_DATE', 'A')
SRC_COL_NAME = ENV('SRC_COL_NAME', 'B')
SRC_COL_PHONE= ENV('SRC_COL_PHONE','C')
SRC_COL_ALIAS= ENV('SRC_COL_ALIAS','D')
SRC_COL_IP   = ENV('SRC_COL_IP',   'E')

TGT_URL       = ENV('TGT_URL')
TGT_TAB       = ENV('TGT_TAB', '시트1')
TGT_IDX_DATE  = int(ENV('TGT_IDX_DATE',  '0'))
TGT_IDX_NAME  = int(ENV('TGT_IDX_NAME',  '1'))
TGT_IDX_PHON  = int(ENV('TGT_IDX_PHON',  '2'))
TGT_IDX_ALIAS = int(ENV('TGT_IDX_ALIAS', '3'))
TGT_IDX_IP    = int(ENV('TGT_IDX_IP',    '4'))

BACKFILL_SCAN_ROWS   = int(ENV('BACKFILL_SCAN_ROWS', '60'))
LOCK_FILENAME        = ENV('LOCK_FILENAME', 'process.lock')
START_JITTER_MAX     = int(ENV('START_JITTER_MAX', '0'))
LOOP_GUARD_SLEEP_SEC = int(ENV('LOOP_GUARD_SLEEP_SEC', '15'))
AUTO_RESTART_ON_ERR  = ENV('AUTO_RESTART_ON_ERROR', 'false').lower() == 'true'
RESTART_DELAY_SEC    = int(ENV('RESTART_DELAY_SEC', '5'))

def tz_now() -> datetime:
    try:
        return datetime.now(KST)
    except Exception:
        return datetime.now().replace(tzinfo=KST)

def fmt_date_dot_kst_today() -> str:
    return tz_now().astimezone(KST).strftime('%Y.%m.%d')

def normalize_phone(phone: str) -> str:
    if phone is None:
        return ''
    return re.sub(r'\D+', '', str(phone))

def a1_range(letter: str, start_row: int, end_row: int) -> str:
    letter = (letter or '').strip().upper()
    return f"{letter}{max(1, int(start_row))}:{letter}{max(1, int(end_row))}"

def chunked(iterable: Iterable, size: int) -> Iterable[List]:
    buf: List = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def _json_path(name: str) -> str:
    return os.path.join(BASE_DIR, name)

def load_json_dict(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            out: Dict[str, str] = {}
            for item in data:
                if isinstance(item, dict):
                    ph = normalize_phone(item.get('phone', ''))
                    ts = item.get('date') or item.get('ts') or item.get('iso')
                    if ph and ts:
                        out[ph] = ts
            return out
    except Exception:
        pass
    return {}

def save_json_dict(path: str, data: Dict) -> None:
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def load_state() -> Dict[str, str]:
    return load_json_dict(_json_path(SYNC_STATE_FILE))

def save_state(d: Dict[str, str]) -> None:
    save_json_dict(_json_path(SYNC_STATE_FILE), d)

def load_cursor() -> Dict[str, int]:
    return load_json_dict(_json_path(CURSOR_STATE_FILE))

def save_cursor(d: Dict[str, int]) -> None:
    save_json_dict(_json_path(CURSOR_STATE_FILE), d)

_RETRY_EXC = (
    RequestsConnectionError,
    RequestsTimeout,
    APIError,
    Exception if isinstance(google_auth_exceptions, type) else google_auth_exceptions.TransportError,
)

def retry(
    op: Callable[[], any],
    tries: int = 5,
    base: float = 0.8,
    cap: float = 60.0,
    label: str = "",
):

    last_err: Optional[Exception] = None
    delay = base
    for i in range(1, tries + 1):
        try:
            return op()
        except APIError as e:
            code = getattr(e.response, "status_code", None)
            msg = str(e)
            if code == 429:
                wait = random.randint(60, 120)
                print(f"[retry/{label}] 429 quota exceeded -> sleep {wait}s")
                time.sleep(wait)
                continue
            if code == 500:
                print(f"[retry/{label}] 500 internal error -> sleep {delay:.2f}s ({msg})")
                time.sleep(delay)
                delay = min(delay * 2, cap)
                continue
            last_err = e
            print(f"[retry/{label}] APIError(not-handled) -> sleep {delay:.2f}s ({msg})")
            if i == tries:
                raise
            time.sleep(delay)
            delay = min(delay * 2, cap)
        except _RETRY_EXC as e:
            last_err = e
            print(f"[retry/{label}] transient -> sleep {delay:.2f}s ({e})")
            if i == tries:
                raise
            time.sleep(delay)
            delay = min(delay * 2, cap)
        except Exception as e:
            last_err = e
            print(f"[retry/{label}] unexpected -> sleep {delay:.2f}s ({e})")
            if i == tries:
                raise
            time.sleep(delay)
            delay = min(delay * 2, cap)
    if last_err:
        raise last_err

_gspread_client: Optional[gspread.Client] = None
_ws_cache: Dict[str, gspread.Worksheet] = {}

def gspread_client() -> gspread.Client:
    global _gspread_client
    if _gspread_client is not None:
        return _gspread_client
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    cred_path = _json_path(GOOGLE_CREDENTIALS_FILENAME)
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scopes)
    _gspread_client = gspread.authorize(creds)
    return _gspread_client

def open_worksheet_by_url_and_tab(url: str, tab: str) -> gspread.Worksheet:
    key = f"{url}|{tab}"
    if key in _ws_cache:
        return _ws_cache[key]

    def _open():
        client = gspread_client()
        sh = client.open_by_url(url)
        ws = sh.worksheet(tab)
        _ws_cache[key] = ws
        return ws

    return retry(_open, tries=5, label=f"open:{tab}")

def read_new_rows(
    ws: gspread.Worksheet,
    last_data_row_1base: int,
    col_letters: Tuple[str, str, str, str, str]
) -> Tuple[int, List[Tuple[str, str, str, str, str]]]:
    def _read_last_row():
        colA = ws.col_values(1)
        try:
            return len(colA or [])
        except Exception:
            return 1

    last_row = retry(_read_last_row, tries=5, label="read_last_row")

    if not isinstance(last_row, int) or last_row < 1:
        last_row = 1

    header_row = 1
    start_data_row = header_row + 1 + int(last_data_row_1base)
    if start_data_row < header_row + 1:
        start_data_row = header_row + 1
    if start_data_row > last_row:
        return last_data_row_1base, []

    end_row = last_row
    A,B,C,D,E = col_letters

    def _batch_get():
        ranges = [
            a1_range(A, start_data_row, end_row),
            a1_range(B, start_data_row, end_row),
            a1_range(C, start_data_row, end_row),
            a1_range(D, start_data_row, end_row),
            a1_range(E, start_data_row, end_row),
        ]
        return ws.batch_get(ranges, major_dimension='COLUMNS')

    try:
        batch = retry(_batch_get, tries=5, label="batch_get")
        cols = []
        for col in batch:
            if not col:
                cols.append([])
            else:
                cols.append(col[0] if isinstance(col[0], list) else (col or []))
        max_len = max((len(c) for c in cols), default=0)
        for i in range(len(cols)):
            if len(cols[i]) < max_len:
                cols[i] += [''] * (max_len - len(cols[i]))
        rows = list(zip(*cols)) if max_len > 0 else []
    except Exception:
        def _fallback_get():
            return ws.get(f"{A}{start_data_row}:{E}{end_row}")
        all_vals = retry(_fallback_get, tries=5, label="fallback_get")
        rows = []
        for r in all_vals or []:
            padded = (r + ['','','','',''])[:5]
            rows.append(tuple(padded))

    newly_processed_count = len(rows)
    new_last_data_row_1base = last_data_row_1base + newly_processed_count
    return new_last_data_row_1base, rows

def within_dup_window(prev_iso: str, window_days: int) -> bool:
    if not prev_iso:
        return False
    try:
        iso = prev_iso.strip().replace('Z', '+00:00')
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_kst = dt.astimezone(KST)
        return (tz_now() - dt_kst) <= timedelta(days=window_days)
    except Exception:
        return False

def backfill_state_from_target(
    tgt_ws: gspread.Worksheet,
    state: Dict[str, str],
    candidate_phone_keys: List[str],
    tgt_idx_phone: int,
    last_n_rows: int = BACKFILL_SCAN_ROWS
) -> int:
    if not candidate_phone_keys:
        return 0

    def _read_last_row():
        colA = tgt_ws.col_values(1)
        try:
            return len(colA or [])
        except Exception:
            return 1
    last_row = retry(_read_last_row, tries=5, label="tgt_last_row")
    if not isinstance(last_row, int) or last_row <= 1:
        return 0

    start = max(2, last_row - last_n_rows + 1)
    end = last_row

    def _get_range():
        return tgt_ws.get(f"A{start}:Z{end}")
    values = retry(_get_range, tries=5, label="tgt_backfill_get") or []

    found_cnt = 0
    cand_set = set(candidate_phone_keys)
    now_iso = tz_now().isoformat()

    for r in values:
        if len(r) <= tgt_idx_phone:
            continue
        shown_phone = r[tgt_idx_phone]
        key = normalize_phone(shown_phone)
        if not key:
            continue
        if key in cand_set and key not in state:
            state[key] = now_iso
            found_cnt += 1

    if found_cnt > 0:
        save_state(state)
    return found_cnt

class SingleInstanceLock:
    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self._fd = None

    def acquire(self) -> bool:
        if os.path.exists(self.lock_path):
            return False
        try:
            self._fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(self._fd, str(os.getpid()).encode('utf-8'))
            return True
        except FileExistsError:
            return False
        except Exception:
            return False

    def release(self):
        try:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            if os.path.exists(self.lock_path):
                os.remove(self.lock_path)
        except Exception:
            pass

def process_once() -> Tuple[int, int]:
    tried = 0
    inserted = 0

    src_ws = open_worksheet_by_url_and_tab(SRC_DOC_URL, SRC_TAB)
    tgt_ws = open_worksheet_by_url_and_tab(TGT_URL, TGT_TAB)

    cursor = load_cursor()
    cursor_key = f"{SRC_DOC_URL}|{SRC_TAB}"
    last_data_row_1base = int(cursor.get(cursor_key, 0))

    state = load_state()

    new_last_data_row_1base, new_rows = read_new_rows(
        src_ws,
        last_data_row_1base,
        (SRC_COL_DATE, SRC_COL_NAME, SRC_COL_PHONE, SRC_COL_ALIAS, SRC_COL_IP)
    )

    if not new_rows:
        cursor[cursor_key] = new_last_data_row_1base
        save_cursor(cursor)
        return tried, inserted

    rows_to_append: List[List[str]] = []
    candidate_phone_keys_for_backfill: List[str] = []

    max_idx = max(TGT_IDX_DATE, TGT_IDX_NAME, TGT_IDX_PHON, TGT_IDX_ALIAS, TGT_IDX_IP)

    batch_seen_phone_keys = set()

    for (src_date, src_name, src_phone, src_alias, src_ip) in new_rows:
        tried += 1

        if (src_name or '').strip() == '테스트':
            continue

        phone_key = normalize_phone(src_phone)
        if not phone_key:
            continue

        prev_iso = state.get(phone_key, '')
        if within_dup_window(prev_iso, DUP_WINDOW_DAYS):
            continue

        if phone_key in batch_seen_phone_keys:
            continue
        batch_seen_phone_keys.add(phone_key)

        tgt_date_out = fmt_date_dot_kst_today()
        tgt_phone_out = (src_phone or '').strip()

        row_out = [''] * (max_idx + 1)
        row_out[TGT_IDX_DATE]  = tgt_date_out
        row_out[TGT_IDX_NAME]  = (src_name or '').strip()
        row_out[TGT_IDX_PHON]  = tgt_phone_out
        row_out[TGT_IDX_ALIAS] = (src_alias or '').strip()
        row_out[TGT_IDX_IP]    = (src_ip or '').strip()
        rows_to_append.append(row_out)

        candidate_phone_keys_for_backfill.append(phone_key)

    if not rows_to_append:
        cursor[cursor_key] = new_last_data_row_1base
        save_cursor(cursor)
        return tried, inserted

    success_count = 0
    now_iso = tz_now().isoformat()
    try:
        for chunk in chunked(rows_to_append, 100):
            def _append():
                return tgt_ws.append_rows(chunk, value_input_option='USER_ENTERED')
            retry(_append, tries=5, label="append_rows")
            for _ in range(len(chunk)):
                if not candidate_phone_keys_for_backfill:
                    break
                key = candidate_phone_keys_for_backfill.pop(0)
                state[key] = now_iso
                success_count += 1
        if success_count > 0:
            save_state(state)
    except Exception as e:
        print(f"[append_rows ERROR] {e}")
        filled = backfill_state_from_target(
            tgt_ws=tgt_ws,
            state=state,
            candidate_phone_keys=candidate_phone_keys_for_backfill.copy(),
            tgt_idx_phone=TGT_IDX_PHON,
            last_n_rows=BACKFILL_SCAN_ROWS
        )
        success_count += filled
        if success_count > 0:
            print(f"[backfill] recovered {filled} items into state")

    inserted = success_count

    cursor[cursor_key] = new_last_data_row_1base
    save_cursor(cursor)

    return tried, inserted

def _force_release_lock(lock_path: str):
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
            print(f"[restart] removed lock: {lock_path}")
    except Exception as e:
        print(f"[restart] lock remove failed: {e}")

def _restart_self(delay_sec: int = 5):
    try:
        print(f"[restart] restarting self in {delay_sec}s ...")
        time.sleep(delay_sec)
        python = sys.executable
        args = [python] + sys.argv
        os.execv(python, args)
    except Exception as e:
        print(f"[restart] exec failed: {e}")
        sys.exit(1)

def main():
    lock = SingleInstanceLock(_json_path(LOCK_FILENAME))
    if not lock.acquire():
        print("[lock] another instance is running. exit.")
        return
    try:
        if START_JITTER_MAX > 0:
            jitter = random.randint(0, START_JITTER_MAX)
            print(f"[start jitter] sleep {jitter}s")
            time.sleep(jitter)

        if POLL_INTERVAL <= 0:
            try:
                tried, inserted = process_once()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] tried={tried}, inserted={inserted}")
            except Exception as e:
                print(f"[fatal single-run error] {e}")
                if AUTO_RESTART_ON_ERR:
                    _force_release_lock(_json_path(LOCK_FILENAME))
                    _restart_self(RESTART_DELAY_SEC)
            return

        while True:
            try:
                tried, inserted = process_once()
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] tried={tried}, inserted={inserted}")
            except Exception as e:
                if AUTO_RESTART_ON_ERR:
                    print(f"[loop guard] error -> auto-restart mode: {e}")
                    _force_release_lock(_json_path(LOCK_FILENAME))
                    _restart_self(RESTART_DELAY_SEC)
                else:
                    print(f"[loop guard] caught error: {e} -> sleep {LOOP_GUARD_SLEEP_SEC}s then continue")
                    time.sleep(LOOP_GUARD_SLEEP_SEC)
                    continue
            time.sleep(POLL_INTERVAL)
    finally:
        lock.release()

if __name__ == '__main__':
    main()
