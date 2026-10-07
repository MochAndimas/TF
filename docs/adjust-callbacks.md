# Adjust install callbacks

POST `/api/adjust/callback?token=<ADJUST_CALLBACK_SECRET>` accepts flat JSON.
The endpoint is disabled until a secret is configured. Set a random secret of at
least 32 bytes and keep it out of source control. `_FILE` is supported. Configure
`ADJUST_APP_TOKENS` (comma separated, no spaces) and
`ADJUST_CALLBACK_ENVIRONMENT=sandbox` for testing, `production` for live.
Use separate deployments/databases for sandbox and live data.

The query token is a shared credential configured by us, not an Adjust signature.
Application request logging excludes this path and the handler strips the query
string from the ASGI scope before Uvicorn access logging; also redact query strings at the
reverse proxy. Optionally configure `ADJUST_CALLBACK_ALLOWED_CIDRS`. Behind a
proxy enforce the official Adjust IP allowlist at ingress and configure Uvicorn
forwarded-header trust only for your proxy. Never trust arbitrary forwarded IPs.

Delivery returns 202 only after SQLite commit. Identical payload retries are
acknowledged without another inbox row. Different payloads for the same app,
environment, Adjust device and install timestamp normalize to one install fact.
First accepted fact wins; a later delivery may fill a NULL campaign_id without changing other attribution fields; attribution updates are not supported in this version.
No date-window delete occurs. Store install metrics remain independent.

Raw JSON is durable in `adjust_callback_raw`; normalized installs are in
`adjust_install`. Times are stored as UTC, `date` is the install reporting date in
Asia/Jakarta. Unavailable optional attribution fields stay NULL. Invalid install
fields mark the inbox row failed; valid rows in the same batch still commit and
the ETL run reports failure. Batches are 200 rows with a fixed high-water ID, so
new incoming callbacks cannot keep one job running forever. Failed rows are not
automatically retried indefinitely.

Apply schema with `python migrate_db.py` (also done on backend Docker start).
Rebuild backend/frontend/scheduler images after code changes. Existing DB URL
selection continues to follow ENV. No automatic raw-data deletion is configured.

Run `python run_scheduled_etl.py --sources adjust_callbacks` to process all pending
callbacks regardless of age. It is also included in the daily default scheduler.
Update Data → Adjust Install Callbacks → Auto does the same. Manual mode replays
all stored callbacks received in the selected WIB date range, including failed
and previously processed rows. Install facts remain idempotent. `rows_loaded`
is the total normalized install count, not newly inserted rows or receipt-window
count. This first version adds ingestion and ETL, not analytics charts.

## Adjust UI: sandbox install

AppView → All apps → select app → Data management → Server callbacks → Add
callback → Guided setup:

- Trigger: Install
- Method: POST
- Endpoint: `https://YOUR-TEST-DOMAIN/api/adjust/callback?token=YOUR-SECRET`
- Condition: `{environment}` equals `sandbox`
- Flat JSON body fields (configure using the key/value editor):

| Key | Value |
| --- | --- |
| app_token | Your actual app token (static value) |
| environment | `{environment}` |
| activity_kind | `{activity_kind}` |
| adid | `{adid}` |
| installed_at | `{installed_at}` |
| created_at | `{created_at}` |
| os_name | `{os_name}` |
| network_name | `{network_name}` |
| campaign_id | `{dcp_campaign_id}` (requires campaign_id on the advertising link) |
| campaign_name | `{campaign_name}` |
| adgroup_name | `{adgroup_name}` |
| creative_name | `{creative_name}` |
| tracker_token | `{tracker}` |
| tracker_name | `{tracker_name}` |

Required for ETL: app_token, environment, activity_kind=install, adid,
installed_at (Unix seconds). Additional flat fields are retained in raw JSON.
No event token or internal user ID is needed for install testing.
Use HTTPS accessible to Adjust; localhost alone is insufficient. Configure
trusted hosts to include your public hostname. Run the mobile SDK in sandbox,
install and open the app, then inspect the device in Adjust Testing Console.
Install means first app open. Use Forget device between repeated install tests.
Check callback delivery, process Update Data Auto, and inspect ETL status.

Production: use the live endpoint/database, production SDK, condition environment
production and `ADJUST_CALLBACK_ENVIRONMENT=production`. Avoid a global callback
that also sends the same install activity unless intentional.

References:
- https://help.adjust.com/en/article/server-callbacks
- https://help.adjust.com/en/article/install-callbacks
- https://help.adjust.com/en/article/conditional-callbacks
- https://help.adjust.com/en/article/adjust-server-ips
- https://help.adjust.com/en/article/testing-console

Campaign ID is optional text (leading zeroes preserved). Organic/unavailable or
unresolved ID placeholders produce NULL. `{campaign_id}` is not assumed to be a
universal Adjust export placeholder. Use DCP: add `campaign_id=<network macro>`
to the advertising Adjust link and `campaign_id={dcp_campaign_id}` to the callback
body. The ad network must populate its macro; adding the callback field alone
does not create an ID. See https://help.adjust.com/en/article/dynamic-callback-parameters.

Google Ads fallback: if explicit campaign_id is absent/unresolved, extract only
a trailing numeric `(ID)` from campaign_name when network_name starts with
Google Ads. Attribution display strings are form-decoded once; raw JSON is
unchanged. Manual replay also refreshes decoded display fields from the original
raw_id, preserving first-delivery attribution for other callbacks. This fallback
does not infer campaign IDs for Organic or other networks. Reference:
https://help.adjust.com/en/article/google-ads-reporting

Install output uses `source_name` instead of `network_name`: google, facebook
(including Meta/Instagram), tiktok, apple, organic, unknown. Other networks use
a lowercase first-word slug. Raw callback and Adjust configuration still use
`network_name`. A trailing numeric campaign ID is removed from campaign_name
only when it matches the separate campaign_id. The development install table
was recreated and replayed from raw storage for this schema change; no new
migration was added.
