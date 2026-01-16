# Alpaca Orders → Google Sheets Exporter

Exports ALL Alpaca orders into a Google Sheet tab named "Orders" (creates it if missing).

## 1) Google setup
1. Create a Google Cloud Service Account.
2. Enable Google Sheets API on that project.
3. Share your target Google Sheet with the service account email (Editor).
4. Put the service account JSON in Railway as base64:
   - `base64 -w 0 service_account.json` (Linux)
   - PowerShell: `[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes((Get-Content .\service_account.json -Raw)))`

Set `GOOGLE_SERVICE_ACCOUNT_JSON_B64` to that value.

## 2) Railway setup
Environment variables:
- ALPACA_API_KEY
- ALPACA_API_SECRET
- ALPACA_PAPER (1 for paper, 0 for live)
- GOOGLE_SHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON_B64

Start Command:
- `python main.py`

Cron Schedule (UTC):
- `0 14,15 * * 1-5`

This triggers at 14:00 and 15:00 UTC on weekdays. The script only runs when it's ~8:00am in America/Denver.

## 3) Local test
Set FORCE_RUN=1 and run:
`python main.py`
