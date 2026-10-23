# Adjust register → Google Sheets

After each successful Adjust callback ETL, including manual runs and the daily
08:00 Asia/Jakarta scheduler, the runner appends previously undelivered
`adjust_register` records. A run with no pending callbacks still retries sheet delivery.

Configure both `FIRST_DEPOSIT_SHEET_ID` and `APP_INSTALL_GSHEET_RANGE` (for example,
`'Sheet2'!A:H`). Leaving the range blank disables delivery; a configured range without a spreadsheet ID fails
the run. `GSHEET_SA_CREDS` is reused with a dedicated write-scoped client; existing
read-only extractors stay unchanged. Share the destination with that service
account as Editor. Destination headers must already match:

| Sheet | Database |
| --- | --- |
| id | tf_id |
| email | email |
| phone | nomor_telepon |
| fullname | username |
| tgl_regis | date (Jakarta reporting date) |
| tracker_token | tracker_token |
| campaign_id | campaign_id |
| source_name | source_name |

Values use RAW strings to preserve identifiers/leading zeroes and prevent formula
interpretation. Missing fields remain blank. Data rows have white backgrounds
and normal text; the header is untouched.

Schema initialization creates `adjust_register_sheet_receipt` through the existing
metadata/create_all bootstrap. Receipts are keyed by destination and event_key,
not tf_id, because tf_id may be absent. Exports are append-only snapshots; later
corrections to an already delivered event do not overwrite the sheet.

The ETL's existing single-run lock serializes deliveries. Do not invoke the sync
outside that lock concurrently or use an independent writer for the same records.
Each run reads the sheet's A:H values and scans facts in batches of 200. Matching
existing rows are adopted, including previously uploaded test data. Multiplicity
is preserved when distinct events have identical exported values. Receipts commit
only after append, read-back verification, and formatting. No SQLite transaction
is held during Google API requests.

Append requests are not blindly retried: if a response is lost after Google has
written the data, the next ETL reconciles existing rows before sending anything.
An export failure marks the ETL failed without removing processed callbacks.
Retry Adjust ETL manually or let the next morning run retry. Successful batches
remain committed. Do not remove or alter delivered rows expecting receipts to
rebuild them; restoring sheet contents is a separate operation.
