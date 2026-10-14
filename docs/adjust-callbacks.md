# Adjust install and lifecycle callbacks

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
Update Data → Adjust Callbacks → Auto does the same. Manual mode replays
all stored callbacks received in the selected WIB date range, including failed
and previously processed rows. Install facts remain idempotent. `rows_loaded`
is the total normalized install plus lifecycle count, not newly inserted rows or receipt-window
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
| gps_adid | `{gps_adid}` |
| idfa | `{idfa}` |
| city | `{city}` |
| country | `{country}` |
| device_name | `{device_name}` |
| os_version | `{os_version}` |
| isp | `{isp}` |
| mcc | `{mcc}` |
| mnc | `{mnc}` |
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

Install device fields are optional: `gps_adid` (Google advertising ID), `idfa`,
`city`, `country`, `device_name`, `os_version`, `isp`, `mcc`, and `mnc`.
`adid` remains the Adjust device ID and part of the install deduplication key.
Blank or unresolved device placeholders become NULL. MCC/MNC are stored as text
so leading zeroes are preserved. Later deliveries may fill missing device fields;
existing non-NULL values are retained. `isp` identifies the internet provider;
MCC/MNC identify the mobile carrier. These are flat JSON fields, not a nested object.
No new schema migration is added: manually back up the database, recreate only
`adjust_install`, and replay the retained raw callbacks before restarting the app.
Older raw callbacks without the new fields will retain NULL for those fields.

## Lifecycle: uninstall, reinstall

The same POST endpoint now accepts `activity_kind` values `install`,
`uninstall`, `reinstall`, `reattribution`, `attribution_update`,
`updated_attribution` (alias), and `reattribution_reinstall`, plus `click` and `impression`. Configure each trigger in Adjust with flat JSON
and the same app/environment/auth settings. Other triggers remain unsupported.
The existing single `adjust_callbacks` source processes all supported activities
sequentially with one handler registry. No additional workers or ETL jobs are used.

Lifecycle facts are stored in `adjust_lifecycle`; install facts stay in
`adjust_install`. The new table is registered with SQLAlchemy metadata and is
created by the existing backend startup `create_all`; no schema migration or
existing-table deletion is needed for lifecycle.

All lifecycle callbacks require `app_token`, `environment`, `activity_kind`,
`adid`, and `created_at` (positive Unix seconds). Occurrence timestamps are:

| Trigger | Required occurrence field | Optional trigger fields |
| --- | --- | --- |
| Uninstall | `uninstalled_at={uninstalled_at}` | `time_to_uninstall={time_to_uninstall}` |
| Reinstall | `reinstalled_at={reinstalled_at}` | `uninstalled_at={uninstalled_at}`, `time_to_reinstall={time_to_reinstall}` |

Store `created_at` separately from `occurred_at`; the latter determines the WIB
reporting date. The lifecycle key hashes app, environment, ADID, activity kind,
and occurrence timestamp. Repeated deliveries and manual replay produce one fact
per occurrence. Different lifecycle timestamps remain separate. Send timestamps
with consistent precision; raw receipts are retained even if equal occurrence
keys merge. Missing/unresolved optional fields become NULL; malformed numeric
values mark the raw callback failed. A later duplicate can fill NULL context,
device or profile fields while preserving existing values and the original raw ID.

Include the same attribution/device keys from the install table above:
`os_name`, `gps_adid`, `idfa`, `city`, `country`, `device_name`, `os_version`,
`isp`, `mcc`, `mnc`, `network_name`, `campaign_id`, `campaign_name`,
`adgroup_name`, `creative_name`, `tracker_token`, and `tracker_name`.
These fields are optional. In particular, Adjust only guarantees ADID as the
device identifier for uninstall; do not require GPS ADID or IDFA.

### Custom user properties

Use exactly `username`, `email`, and `nomor_telepon` as custom parameter keys
in the mobile SDK and callback payload. These are not built-in Adjust placeholders.
The API accepts the flat keys in JSON or, for SDK-appended callback parameters,
in the query string. A populated JSON value takes precedence over a query value.
The query credential and profile parameters are redacted from app access logs.
No `user_prop` object is required or accepted. Phone numbers and email addresses
are kept as text, including `+` and leading zeroes; JSON profile values are not
form-decoded. Values must be supplied by the app; they are not automatically
inferred from the device. Session parameters may not be available for uninstall
or reinstall. No profile enrichment from prior callbacks is performed.

References:
- https://help.adjust.com/en/article/uninstall-callbacks
- https://help.adjust.com/en/article/reinstall-and-reinstall-attribution-callbacks
- https://help.adjust.com/en/article/custom-callback-parameters
- https://dev.adjust.com/en/sdk/android/features/session-parameters/

For a local dashboard with an ngrok callback tunnel, keep
`BACKEND_PUBLIC_URL=http://localhost:5505` for browser authentication and set
`EXTRA_TRUSTED_HOSTS=<your-ngrok-hostname>` to allow the callback Host header.
This avoids routing browser login through the external tunnel.

## Attribution history

Attribution callbacks use the same endpoint, raw inbox and sequential ETL registry.
Facts go to `adjust_attribution_history`; original install and lifecycle facts
are not updated. The table is created by existing metadata bootstrap; no new
migration or table deletion is needed.

The device, location, provider, attribution and custom profile fields are the
same as lifecycle (`username`, `email`, `nomor_telepon` remain SDK custom keys).
Required common fields are app_token, environment, activity_kind, adid and
created_at. Configure these activity timestamps:

| Trigger | Occurrence timestamp | Additional fields |
| --- | --- | --- |
| Reattribution | `reattributed_at={reattributed_at}` | `first_tracker_token={first_tracker}`, `last_tracker_token={last_tracker}`, `last_tracker_name={last_tracker_name}` |
| Updated attribution | `attribution_updated_at={attribution_updated_at}` | `outdated_tracker_token={outdated_tracker}`, `outdated_tracker_name={outdated_tracker_name}` |
| Reattribution reinstall | `reattributed_at={reattributed_at}` | required `reinstalled_at={reinstalled_at}`; optional `uninstalled_at={uninstalled_at}`, `time_to_reinstall={time_to_reinstall}`, first/last tracker fields |

`attribution_update` is the canonical history kind; `updated_attribution` is
accepted as an alias. History keys include app, environment, ADID, canonical
activity and occurrence timestamp. Updated-attribution keys additionally include
current/outdated tracker tokens to preserve distinct transitions within a second.
Reattribution-reinstall keys also include reinstall timestamp. Configure these
identity fields consistently across duplicate deliveries. Existing non-NULL
context is retained; duplicates may fill NULL optional fields. Replay is idempotent.
The WIB date follows occurrence time, independent of receipt and created_at.

References:
- https://help.adjust.com/en/article/m-reattribution-callbacks
- https://help.adjust.com/en/article/updated-attribution-callbacks
- https://help.adjust.com/en/article/reinstall-and-reinstall-attribution-callbacks

## Engagement: click and impression

Click and impression callbacks use the existing endpoint and sequential
`adjust_callbacks` run. `adjust_engagement` is registered in metadata and created
by backend startup, with no migration or table deletion.

Required fields: app_token, environment, activity_kind and created_at (Unix
seconds). Device identifiers, attribution, location/provider and the custom
username/email/nomor_telepon fields are the same as lifecycle but optional.
ADID is not required for pre-install engagements. No identity is inferred if all
advertising/device IDs are unavailable.

Use `click_time={click_time}` for Click and
`impression_time={impression_time}` for Impression. The corresponding timestamp
becomes occurred_at and determines the WIB reporting date. If missing or an
unresolved placeholder, created_at is used; a present malformed timestamp fails
DQ. Both source timestamps remain available as nullable columns.

The engagement key is the canonical hash of the complete stored raw payload.
Exact duplicate deliveries and manual replays produce one engagement fact.
Different payloads are retained even for the same device/timestamp: there is no
assumed unique engagement ID and no guarantee of semantic deduplication across
varying payloads. Two completely identical legitimate activities are likewise
indistinguishable with these placeholders. Configure consistently and retain raw
receipts; a network-specific stable engagement ID can be added when available.
Replay refreshes transformed fields from that raw payload without changing the
original raw_id. These callbacks do not update install/lifecycle/attribution history.

References:
- https://help.adjust.com/en/article/click-callbacks
- https://help.adjust.com/en/article/impression-callbacks

## Business events

`activity_kind=event` is dispatched to `adjust_event`. Configure callbacks for
these exact Adjust event names, selecting each event in the trigger setup:

| Adjust event_name | Stored event_type |
| --- | --- |
| Register Completed | register |
| Deposit Completed | deposit |
| First Deposit Completed | first_deposit |
| Subscribe Success | subscribe_success |

Include `event_token={event}`, `event_name={event_name}` and
`created_at={created_at}` in each callback. App, environment, ADID, event token,
recognized event name and created_at are required. Display event names are decoded
using the same Adjust form-decoding rules. Unknown names remain durable in raw
but fail normalization; only the four mapped business events are loaded.

Device/location/provider/attribution and username/email/nomor_telepon are the
same optional fields as lifecycle. `transaction_id` is an optional custom SDK
callback parameter (not an assumed built-in placeholder), accepted in flat JSON
or query parameters. Send the same stable transaction ID on retries; distinct
payments need distinct IDs. Register events may also provide an occurrence ID
via this parameter. Deduplication uses app + environment + event token +
transaction_id when available. This preserves separate Deposit and First Deposit
facts for one transaction because their Adjust event tokens differ. Do not sum
those two event types together as total deposit revenue.

Without transaction_id, the key hashes the complete raw payload, as for engagement.
Only exact-payload deduplication is guaranteed, not semantic payment deduplication.
Existing non-NULL values are retained; duplicates may fill missing context and
revenue. Amount and currency are enriched together. No cross-table business or
install records are updated. Manual replay is idempotent.

For monetary events, use `revenue_amount={revenue_float}` and
`currency={currency}`. The former is in whole currency units, not cents. It is
optional: the mobile SDK must send revenue for Adjust to populate it. Missing or
unresolved amount placeholders become NULL. Present values must be finite,
nonnegative decimals; a populated amount requires a three-letter currency.
Amounts are stored as exact decimal text rather than SQLite binary floating
point. Native incoming `revenue_float` is also accepted. Register does not
require revenue. The WIB date follows created_at; occurred_at stores its UTC time.

The new table is registered in metadata and created on backend startup, with
no new schema migration or deletion of existing tables. `rows_loaded` includes
business facts through the shared registry; processing remains sequential.

References:
- https://help.adjust.com/en/article/event-callbacks
- https://help.adjust.com/en/article/custom-callback-parameters
- https://help.adjust.com/en/partner-setup/ingenioustech


## Separate business event tables

Business events now route to `adjust_register`, `adjust_deposit`, `adjust_first_deposit`, and `adjust_subscribe_success`. Each keeps the shared event/device/attribution columns and a `custom_parameters` JSON object containing every additional received key with its original value. Subscribe Success also exposes the currently observed subscription/channel metrics as nullable text columns, preserving source precision and formatting. New custom keys remain available in JSON without schema changes. Custom URL query parameters are captured except the callback authentication token; nonempty body values take precedence.

The legacy `adjust_event` table and model have been removed; business events use the four separate tables. Existing endpoint and callback URIs remain valid.

## Ad spend: adjust_cost

Select the Ad spend callback trigger and reuse `/api/adjust/callback`. Accepted activity kinds are `cost_update` and `cost_updated` (both spellings appear in Adjust documentation), normalized to `cost_update`. Required fields: app_token, environment, created_at. Device IDs and profile fields are optional. Include cost_type, cost_amount, cost_currency, reporting_cost, cost_id_md5 plus the existing device/location/attribution placeholders. Profile custom keys remain username, email, nomor_telepon, supplied by the integration rather than invented built-in placeholders.

Amounts are nullable exact decimal strings; supplied amounts must be finite and nonnegative. cost_currency is uppercased and required when cost_amount is present. Reporting cost is in the app reporting currency, separate from cost_currency. Missing placeholders are retained as NULL; this does not imply zero spend. Additional received keys are retained in custom_parameters JSON.

Deduplication uses the complete raw payload hash, so exact delivery retries/replays remain one row while different updates for the same cost_id_md5 remain separate history rows. Do not sum all history rows as final spend without resolving network updates/revisions. This handler covers dedicated ad spend callbacks; cost carried on install/click/impression callbacks is not additionally routed here.

The dedicated Ad spend trigger supports networks using Partner Ad Spend Push API. AdSpend on Engagement instead uses click/impression/install/global callbacks. Raw user-level ad spend is unavailable for self-attributing networks. Sources: https://help.adjust.com/en/article/ad-spend-callbacks and https://help.adjust.com/en/article/ad-spend-reporting-solution.


Session callbacks are no longer accepted or processed. Existing session raw and lifecycle rows have been deleted.

## Admin CSV exports

Settings → Export Data Adjust (`?page=export-data-adjust`) provides an allowlisted table selector, inclusive WIB date range, row-count preview and actual data date bounds. Raw callback exports are excluded; normalized tables use their reporting date. Empty ranges cannot be prepared. All columns, including JSON custom parameters, are exported in UTF-8 CSV with BOM. Spreadsheet formula-like scalar strings are escaped with a leading apostrophe.

Both the UI and `/api/adjust-export/{tables,summary,csv}` require an exact admin or superadmin role; analyst and other roles are denied, including direct API calls. This restriction deliberately bypasses the app's legacy admin-to-analyst alias. Preview counts can change as callbacks arrive; the prepared CSV shows its actual exported row count. Large backend exports spool to temporary disk and release their SQLite read transaction before download.
