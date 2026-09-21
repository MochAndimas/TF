# Data Socmed ETL

Source key: `data_socmed`. Destination: `data_socmed`.

Configuration uses `DATA_SOCMED_GSHEET` (spreadsheet ID),
`DATA_SOCMED_GSHEET_RANGE` (A1 range including the header row), and the shared
read-only service account in `GSHEET_SA_CREDS`.

Only these fields are retained in staging and the destination:

| Sheet header | Database column |
| --- | --- |
| tgl_regis | tgl_regis |
| id | id |
| fullname | fullname |
| utm_source | utm_source |
| utm_medium | utm_medium |
| tag | tag |
| Status New / Existing | user_status |
| First Depo $ | first_depo |
| Time To Closing | time_to_closing |
| First Depo Date | first_depo_date |

Header matching ignores case and whitespace, including newlines. IDs are stored
as text to preserve leading zeros. Only `utm_source` values `instagram`, `youtube`,
`tiktok`, and `facebook` enter the final table (exact match ignoring case and
surrounding whitespace). Empty and other sources are excluded before validation.
Staging retains the selected fields from all source rows for auditing.
All tags and statuses are retained; zero and
blank deposits are accepted, with blanks stored as NULL. Invalid amounts,
negative amounts, invalid dates, missing IDs, and duplicate `(tgl_regis, id)`
keys fail validation. No extra metadata columns are added to the final table.

Run `python migrate_db.py` to apply the additive table migration. Select
**Data Socmed (GSheet)** on the Update Data page to run a manual date range.
The source is also included in the default daily scheduler, or can run alone:

```sh
python run_scheduled_etl.py --sources data_socmed
```

Automatic runs refresh H-7 through H-1 by registration date. Use a manual range
for initial historical loading or changes to older registrations. Each run reads
the configured sheet range, stages the selected fields, and replaces the target
registration-date window in one transaction. An empty result clears that window,
following the shared ETL runner behavior. Validation failure preserves final data.

`first_depo_date` is nullable. Blank, zero, and spreadsheet placeholder dates
before 1900 are stored as NULL; malformed dates fail validation. Existing rows
receive NULL until their registration-date window is refreshed.

Revenue comparisons follow the selected preset: This Month compares against the
same day range in the previous calendar month (capped at that month’s last day);
Last Month compares against the entire preceding calendar month. Last 7/30 Day
and Custom Range compare against the immediately preceding equal-length window.
Each revenue card displays the actual comparison dates. Both periods are filtered
by the page platform and registration date.
