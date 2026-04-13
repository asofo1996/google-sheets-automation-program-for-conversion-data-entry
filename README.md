# Google Sheets Client Sync

A Python automation script that reads new rows from a source Google Sheet and appends them to a target Google Sheet with duplicate protection, retry handling, state tracking, and optional auto-restart.

## Features

- Syncs newly added rows from a source Google Sheet to a target Google Sheet
- Tracks the last processed row using a cursor state file
- Prevents duplicate insertion based on normalized phone numbers
- Supports duplicate window control with configurable days
- Stores sync history in a local JSON state file
- Uses retry logic for temporary Google Sheets API and connection errors
- Prevents multiple instances from running at the same time using a lock file
- Supports optional auto-restart mode when runtime errors occur
- Writes output dates in Korea Standard Time (`YYYY.MM.DD` format)

## How It Works

This script continuously polls a source worksheet, reads only the rows that have not yet been processed, filters invalid or duplicate rows, and appends valid rows to a target worksheet.

It uses:

- `.env` for configuration
- `credentials.json` for Google service account authentication
- `client_sync_state.json` for phone-based duplicate tracking
- `cursor_state.json` for remembering the last processed row
- `process.lock` to prevent running multiple instances at once

## File

- `gsheets_client_sync.py` — main sync script

## Requirements

Install the required packages:

```bash
pip install gspread oauth2client python-dotenv requests
```

## Environment Variables

Create a `.env` file in the same directory as the script.

Example:

```env
GOOGLE_CREDENTIALS_FILENAME=credentials.json

POLL_INTERVAL=180
TIMEZONE=Asia/Seoul
SYNC_STATE_FILE=client_sync_state.json
CURSOR_STATE_FILE=cursor_state.json
DUP_WINDOW_DAYS=30

SRC_DOC_URL=https://docs.google.com/spreadsheets/d/your_source_sheet_id/edit
SRC_TAB=시트1
SRC_COL_DATE=A
SRC_COL_NAME=B
SRC_COL_PHONE=C
SRC_COL_ALIAS=D
SRC_COL_IP=E

TGT_URL=https://docs.google.com/spreadsheets/d/your_target_sheet_id/edit
TGT_TAB=시트1
TGT_IDX_DATE=0
TGT_IDX_NAME=1
TGT_IDX_PHON=2
TGT_IDX_ALIAS=3
TGT_IDX_IP=4

BACKFILL_SCAN_ROWS=60
LOCK_FILENAME=process.lock
START_JITTER_MAX=0
LOOP_GUARD_SLEEP_SEC=15
AUTO_RESTART_ON_ERROR=false
RESTART_DELAY_SEC=5
```

## Source Sheet Column Mapping

The script reads the source sheet using column letters:

- `SRC_COL_DATE` → date column
- `SRC_COL_NAME` → client name
- `SRC_COL_PHONE` → phone number
- `SRC_COL_ALIAS` → alias or campaign name
- `SRC_COL_IP` → IP address

## Target Sheet Column Mapping

The script writes data to the target sheet using zero-based column indexes:

- `TGT_IDX_DATE` → output date
- `TGT_IDX_NAME` → client name
- `TGT_IDX_PHON` → phone number
- `TGT_IDX_ALIAS` → alias or campaign name
- `TGT_IDX_IP` → IP address

Example:

If your target sheet columns are:

- A = Date
- B = Name
- C = Phone
- D = Alias
- E = IP

Then use:

```env
TGT_IDX_DATE=0
TGT_IDX_NAME=1
TGT_IDX_PHON=2
TGT_IDX_ALIAS=3
TGT_IDX_IP=4
```

## Authentication

This project uses a Google service account.

Steps:

1. Create a Google Cloud project
2. Enable Google Sheets API and Google Drive API
3. Create a service account
4. Download the service account JSON file
5. Save it as `credentials.json`
6. Share both source and target Google Sheets with the service account email

## Run

```bash
python gsheets_client_sync.py
```

## Single Run Mode

If `POLL_INTERVAL` is set to `0` or lower, the script runs only once and exits.

Example:

```env
POLL_INTERVAL=0
```

## Duplicate Handling

Phone numbers are normalized by removing non-numeric characters before comparison.

A row is skipped when:

- the name is `테스트`
- the phone number is empty
- the phone number already exists in the local sync state within the configured duplicate window
- the same phone number appears multiple times in the same batch

## Retry and Recovery

The script includes retry handling for temporary failures such as:

- Google Sheets API quota issues
- temporary server errors
- network timeouts
- connection failures

If appending rows fails after partial progress, it attempts to recover sync state by scanning recent rows from the target sheet.

## Lock File

To prevent duplicate execution, the script creates a lock file:

```text
process.lock
```

If another instance is already running, the script exits.

## Output Example

Typical console output:

```text
[2026-04-13 21:30:00] tried=5, inserted=3
```

## Important Security Note

Do **not** upload the following files to a public GitHub repository:

- `.env`
- `credentials.json`
- `client_sync_state.json`
- `cursor_state.json`
- `process.lock`

## Recommended `.gitignore`

```gitignore
.env
credentials.json
client_sync_state.json
cursor_state.json
process.lock
__pycache__/
*.pyc
venv/
```

## Project Purpose

This script is useful when you need to:

- automatically transfer lead or conversion data between Google Sheets
- prevent duplicate client insertion by phone number
- maintain lightweight local sync history
- operate a simple polling-based sheet automation workflow without a full database

## Notes

- Date output is generated in Korea Standard Time
- Source and target spreadsheet structure must match your `.env` settings
- The script assumes the first row is a header row
- The Google service account must have access to both spreadsheets

## License

This project is for private/internal automation use unless otherwise specified.
