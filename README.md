# Traders Family Dashboard

Dashboard internal untuk memantau performa campaign, acquisition, active users, registrasi, install aplikasi, deposit, dan media sosial organik. Repo ini menggabungkan FastAPI sebagai backend API, Streamlit sebagai dashboard UI, ETL pipeline untuk sinkronisasi data, serta scheduler berbasis cron untuk update harian.

## Ringkasan Arsitektur

Project ini berjalan sebagai single-node application dengan SQLite file-based storage. Arsitektur yang dipakai saat ini:

- `FastAPI` untuk authentication, authorization, analytics API, ETL trigger, OAuth callback, dan endpoint operational.
- `Streamlit` untuk login, session restore, role-based navigation, visualisasi analytics, account management, dan manual ETL trigger.
- `ETL layer` untuk ingest data iklan, GA4, register/deposit, media sosial, Play Console, dan App Store Connect.
- `Cron scheduler` untuk menjalankan ETL harian secara serial.
- `SQLite` sebagai primary datastore untuk user, token/session, ETL run, request log, managed secret, dan data analytics.

Karena database utama masih SQLite, ada constraint operasional yang memang disengaja:

- Backend harus dijalankan dengan `WORKERS=1`.
- ETL write tidak boleh overlap liar.
- Inisialisasi schema dibuat explicit lewat `python init_db.py` atau service `db-init`.
- Backend Docker menjalankan `python migrate_db.py` sebelum start agar schema existing ikut ter-update saat container backend restart/recreate.
- Deployment diasumsikan single host, bukan multi-node shared database.

## Komponen Utama

### 1. Backend FastAPI

Backend entrypoint ada di `main.py` dan bootstrap utama ada di `app/utils/app_utils.py`.

Tanggung jawab utamanya:

- login, logout, refresh token, session restore
- account CRUD untuk `superadmin`
- endpoint analytics untuk overview, campaign, deposit, activity, install, dan media sosial
- trigger manual ETL dan polling status ETL
- summary ETL untuk status operasional, termasuk durasi, row count, dan quality report
- Google Ads, YouTube, dan TikTok OAuth callback
- Meta Ads token exchange/status serta Instagram token exchange/save/refresh
- request logging dan healthcheck

Endpoint penting:

- `POST /api/login`
- `POST /api/token/refresh`
- `POST /api/logout`
- `GET /api/accounts`
- `POST /api/register`
- `GET /api/overview/active-users`
- `GET /api/overview/campaign-cost`
- `GET /api/campaign/user-acquisition`
- `GET /api/campaign/brand-awareness`
- `GET /api/campaign/remarketing`
- `GET /api/campaign/internal-register`
- `GET /api/campaign/all-source-register`
- `GET /api/campaign/login-activity`
- `GET /api/install/analytics`
- `GET /api/instagram/analytics`
- `GET /api/facebook/analytics`
- `GET /api/tiktok/analytics`
- `GET /api/youtube/analytics`
- `GET /api/deposit/daily-report`
- `GET /api/deposit/ba-report`
- `GET /api/deposit/remarketing-report`
- `GET /api/sqlite-maintenance/status`
- `POST /api/sqlite-maintenance/vacuum`
- `POST /api/feature-data/update-external-api`
- `GET /api/feature-data/update-external-api/summary`
- `GET /api/feature-data/update-external-api/{run_id}`
- `POST /api/google-ads/oauth/start` (GET juga tersedia untuk redirect)
- `GET /api/google-ads/oauth/callback`
- `GET /api/meta-ads/token/status`
- `POST /api/meta-ads/token/exchange`
- `POST /api/youtube/oauth/start`
- `GET /api/youtube/oauth/callback`
- `POST /api/tiktok/oauth/start`
- `GET /api/tiktok/oauth/callback`
- `GET /terms` dan `GET /privacy`
- `GET /health`

Walaupun kode berada di folder `api/v1`, router saat ini menggunakan URL `/api/...`.

### 2. Frontend Streamlit

Frontend entrypoint ada di `streamlit_run.py`. UI Streamlit ini bukan sekadar dashboard chart, tapi juga shell aplikasi internal:

- login form
- browser-based auth bridge untuk cookie-aware login/refresh/logout
- restore session dari `HttpOnly` cookie
- role-based sidebar navigation
- halaman analytics
- halaman account management
- halaman manual update data
- halaman konfigurasi token Google Ads, Meta Ads, Instagram, TikTok, dan YouTube
- halaman database maintenance dan dokumen legal

Konfigurasi `PUBLIC_PAGE_KEYS` memuat `google_ads_token`, `meta_ads_token`,
`instagram_token`, `tiktok_token`, `youtube_token`, `terms`, dan `privacy`.
Daftar ini dipakai saat dispatch halaman; tidak berarti semua URL halaman token
bisa langsung diakses anonim.

Resolver sebelum login menangani `terms`/`privacy` (path atau query `page`) dan
parameter callback OAuth yang diarahkan ke halaman Google Ads Token. Navigasi
Settings serta operasi backend untuk memulai OAuth atau mengelola token tetap
memerlukan akun `superadmin`. Callback Google Ads, YouTube, dan TikTok tersedia
di backend dan memvalidasi OAuth state; TikTok juga menggunakan PKCE.

### 3. ETL dan Scheduler

Alur ETL dirakit di:

- `app/etl/extract.py`
- `app/etl/transform.py`
- `app/etl/quality.py`
- `app/etl/staging.py`
- `app/etl/load.py`
- `app/etl/pipelines.py`
- `app/etl/job_runner.py`

Source ETL terjadwal default, sesuai urutan `DEFAULT_SCHEDULED_SOURCES` di
`app/etl/job_runner.py`:

- `google_ads`
- `facebook_ads`
- `tiktok_ads`
- `unique_campaign`
- `ga4_daily_metrics`
- `instagram_insights`
- `instagram_media_insights`
- `tiktok_insights`
- `tiktok_media_insights`
- `youtube_daily_insight`
- `youtube_media_insight`
- `facebook_page_insights`
- `facebook_page_media_insights`
- `daily_register`
- `regis_utm_daily`
- `first_deposit`
- `first_deposit_ba`
- `ms_deposit`
- `all_depo` — daily ALL DEPO aggregates; one row per date, register quantity and total/first-deposit quantities and amounts, split by auto closing and consultant. Manual updates replace the selected dates; auto refreshes H-7 through H-1. Values retain the source sheet units.
- `play_console_install_metrics`
- `apple_install`

Google Ads dan Facebook Ads memakai API provider; TikTok Ads memakai Google
Sheets. Integrasi TikTok organik memakai API TikTok. Play Console membaca export
CSV dari GCS, sedangkan Apple membaca report App Store Connect.

Executor tambahan `apple_install_snapshot` dan `apple_report_request` tidak
masuk jadwal default. Endpoint membatasi Apple snapshot ke mode manual.

Auto window umumnya H-7 sampai H-1. TikTok account insights memakai snapshot hari
ini, sedangkan Apple install auto memakai H-5. Manual memakai rentang eksplisit.
Pipeline menyimpan raw staging, melakukan transform/validasi, lalu mengganti
window data target dalam transaksi. Hasil kosong pada shared runner juga
mengosongkan window tersebut.

Scheduler harian:

- source cron: `scripts/cron/traders_family_etl.cron`
- wrapper: `scripts/run_scheduled_etl.sh`
- CLI runner: `run_scheduled_etl.py`
- container entrypoint: `docker/scheduler-entrypoint.sh`

Secara default cron dijalankan setiap hari pukul `08:00 Asia/Jakarta`.
CLI menunggu setiap source selesai dan melanjutkan source berikutnya saat ada
kegagalan, kecuali dijalankan dengan `--fail-fast`.

Setiap ETL run dicatat di tabel `etl_run` dengan metadata operasional:

- `status`
- `window_start` dan `window_end`
- `rows_loaded`
- `duration_ms`
- `quality_report`
- `error_detail` bila gagal

Manual ETL dari dashboard hanya menerima satu source per run. Untuk menjalankan
beberapa source, jalankan source tersebut satu per satu agar status, locking, dan
error handling tetap jelas di SQLite single-node deployment. Status berjalan
dari `queued` ke `running`, lalu `success` atau `failed`. Job manual menggunakan
`asyncio.create_task` dalam proses backend; restart backend dapat memutus job.
Trigger berikutnya menandai run aktif yang berumur lebih dari 6 jam sebagai failed.
`rows_loaded` merupakan jumlah row pada window target setelah job; bukan jumlah
insert baru. Quality report merangkum hasil job dan exception validasi.

## Fitur yang Sudah Ada

### Authentication dan Session

- bearer access token untuk API call
- pembaruan access/refresh token; session handle cookie tetap sama saat refresh
- persistent auth session via cookie `tf_session` saat remember-me aktif
- session restore dari browser
- logout satu sesi
- logout semua sesi
- audit event untuk auth/security
- temporary throttling untuk login dan token refresh

### Security Hardening

- password policy minimum
- token tidak disimpan plaintext di database
- session/token difingerprint sebelum dipersist
- secret eksternal bisa dienkripsi di database
- CORS explicit allowlist
- trusted host middleware
- `HttpOnly` auth cookie
- security headers seperti CSP, HSTS, X-Frame-Options, dan lain-lain
- support `*_FILE` untuk secret injection dari mounted file

### Dashboard dan Operasi

- overview analytics
- campaign analytics untuk user acquisition, brand awareness, dan remarketing
- first deposit, first deposit BA, dan remarketing deposit report
  - First Deposit BA berada di Revenue (`?page=first-deposit-ba`) dan membaca `data_depo_ba`, dengan filter, metrik, serta izin akses yang sama seperti First Deposit.
- register, login activity, serta install Android/iOS
- Instagram, Facebook, TikTok, dan YouTube analytics
- account management untuk `superadmin`
- manual ETL trigger dengan status polling
- healthcheck backend
- backup helper untuk SQLite

## Role Access

Akses navigasi diatur di `streamlit_app/app_shell/config.py`; backend memeriksa
role pada setiap endpoint dengan helper `app/utils/rbac.py` atau guard khusus.

| Role | Akses navigasi |
| --- | --- |
| `superadmin` | Semua halaman, termasuk Settings. |
| `analyst` | Home, Overall, Revenue, Campaign, Socmed, dan Activity. |
| `tech_it` | Home, Overall, Revenue, Campaign, Socmed, dan Activity. |
| `digital_marketing` | Home, Overall, Campaign, Socmed, dan Activity. |
| `finance` | Home, Overall, Revenue, dan Campaign. |
| `social_media` | Home dan Socmed. |
| `sales` | Role dikenal, belum memiliki navigasi aktif. |

Ketiga endpoint Campaign (User Acquisition, Brand Awareness, Remarketing)
mengizinkan role analytics, termasuk `digital_marketing`, serta `finance`.
Endpoint Revenue hanya mengizinkan `superadmin`, `analyst`, `tech_it`, dan
`finance` (beserta alias legacy `admin`).

Role legacy `admin` dinormalisasi menjadi `analyst` oleh helper RBAC umum.
Pengecualian: guard endpoint Install memakai role exact dan tidak memasukkan
`admin`; daftar navigasi legacy `admin` juga tidak memuat Install.

## Struktur Repo

```text
.
|- app/
|  |- api/v1/endpoint/          # endpoint FastAPI
|  |- api/v1/functions/         # payload builder untuk endpoint layer
|  |- core/                     # settings dan security helpers
|  |- db/                       # session, models, schema bootstrap
|  |- etl/                      # extract, transform, quality, staging, load
|  |- schemas/                  # schema request/response
|  |- services/                 # orkestrasi auth
|  |- utils/                    # service layer dan app bootstrap helpers
|- docker/
|  |- scheduler-entrypoint.sh   # bootstrap cron di container scheduler
|- scripts/
|  |- cron/                     # jadwal cron utama
|  |- backup_sqlite.py          # SQLite backup helper
|  |- run_scheduled_etl.sh      # shell wrapper ETL terjadwal
|- streamlit_app/
|  |- app_shell/                # config navigasi dan session bootstrap
|  |- components/               # auth bridge custom component
|  |- functions/                # helper API, UI, session, charting
|  |- page/                     # halaman-halaman Streamlit
|- init_db.py                   # explicit database initialization
|- main.py                      # FastAPI entrypoint
|- run_scheduled_etl.py         # CLI ETL runner
|- streamlit_run.py             # Streamlit entrypoint
|- Dockerfile
|- docker-compose.yml
```

## Requirements

- Python `3.12`
- `pip`
- akses ke environment variable/secret yang dibutuhkan
- untuk Docker flow: Docker dan Docker Compose

## Konfigurasi Environment

Project membaca konfigurasi dari environment/`.env` lewat `python-decouple`.
Setting yang memakai `_read_setting` di `app/core/config.py` mendukung `*_FILE`.
Integrasi yang membaca `config` langsung tidak otomatis mendukung pola tersebut.
Gunakan `.env.example` sebagai template dan sesuaikan key dengan integrasi yang dipakai.

Contoh:

- `JWT_SECRET_KEY` atau `JWT_SECRET_KEY_FILE`
- `JWT_REFRESH_SECRET_KEY` atau `JWT_REFRESH_SECRET_KEY_FILE`
- `CSRF_SECRET` atau `CSRF_SECRET_FILE`
- `APP_ENCRYPTION_KEY` atau `APP_ENCRYPTION_KEY_FILE`
- `INITIAL_SUPERADMIN_PASSWORD` atau `INITIAL_SUPERADMIN_PASSWORD_FILE`

### Variabel Inti Backend

- `ENV`
- `DEV_HOST`
- `DEV_PORT`
- `HOST`
- `PORT`
- `WORKERS`
- `DEV_DB_URL`
- `DB_URL`
- `FRONTEND_URL`
- `BACKEND_PUBLIC_URL`
- `CSRF_SECRET`
- `JWT_SECRET_KEY`
- `JWT_REFRESH_SECRET_KEY`
- `APP_ENCRYPTION_KEY`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `REFRESH_TOKEN_EXPIRE_DAYS`
- `BOOTSTRAP_SUPERADMIN`
- `INITIAL_SUPERADMIN_NAME`
- `INITIAL_SUPERADMIN_EMAIL`
- `INITIAL_SUPERADMIN_PASSWORD`
- `AUTO_INIT_DB_ON_STARTUP`
- `SQLITE_BUSY_TIMEOUT_MS`
- `ALLOW_CONCURRENT_ETL_RUNS`
- `REQUEST_LOG_QUEUE_MAX_SIZE`
- `REQUEST_LOG_FLUSH_BATCH_SIZE`
- `REQUEST_LOG_FLUSH_INTERVAL_SECONDS`

### Variabel Integrasi External Service

- `GSHEET_SA_CREDS`
- `GSHEET_SHEET_ID`
- `FIRST_DEPOSIT_SHEET_ID`
- `FIRST_DEPOSIT_SHEET_RANGE`
- `MS_DEPOSIT_SHEET_RANGE`
- `ALL_DEPO_GSHEET_ID`
- `ALL_DEPO_GSHEET_RANGE` (for example `'ALL DEPO'!A:N`; uses `GSHEET_SA_CREDS`)
- `DAILY_REGIS_SHEET_ID`
- `DAILY_REGIS_SHEET_RANGE`
- `REGIS_UTM_GHSEET_SHEET_ID`
- `REGIS_UTM_SHEET_RANGE`
- `GA4_PROPERTY_ID`
- `GA4_SA_CREDS`
- `GOOGLE_ADS_DEVELOPER_TOKEN`
- `GOOGLE_ADS_CLIENT_ID`
- `GOOGLE_ADS_CLIENT_SECRET`
- `GOOGLE_ADS_LOGIN_CUSTOMER_ID`
- `GOOGLE_ADS_CUSTOMER_ID`
- `GOOGLE_ADS_REDIRECT_URI`
- `META_APP_ID`
- `META_APP_SECRET`
- `META_API_VERSION`
- `META_AD_ID`
- `FB_PAGE_ID`
- `INSTAGRAM_USER_ID`
- `INSTAGRAM_MEDIA_INSIGHT_CONCURRENCY`
- `INSTAGRAM_FOLLOW_ACTIVITY_CONCURRENCY`
- `YOUTUBE_CLIENT_ID`
- `YOUTUBE_CLIENT_SECRET`
- `YOUTUBE_REDIRECT_URI`
- `YOUTUBE_CHANNEL_ID`
- `YOUTUBE_MEDIA_INSIGHT_CONCURRENCY`
- `TIKTOK_CLIENT_KEY`, `TIKTOK_CLIENT_SECRET`, `TIKTOK_REDIRECT_URI`, `TIKTOK_SCOPES`
- `TIKTOK_VIDEO_LIST_MAX_PAGES`
- `PLAY_CONSOLE_SA`, `PLAY_CONSOLE_NAME_APP`, `PLAY_CONSOLE_REPORT_BUCKET`, `PLAY_CONSOLE_REPORT_PREFIXES`
- `APPLE_ASC_KEY_ID`, `APPLE_ASC_ISSUER_ID`, `APPLE_ASC_PRIVATE_KEY`, `APPLE_ASC_APP_ID`, `APPLE_ASC_REPORT_REQUEST_ID`
- `USD_TO_IDR_RATE` (default kode: 16000; konversi deposit pada overview)

### Variabel Tambahan untuk Streamlit dan Docker

- `FRONTEND_PORT`: port Streamlit dalam Compose, misalnya `5504`
- `STREAMLIT_API_HOST`
  dipakai agar container Streamlit memanggil backend internal Docker, misalnya `http://backend:8000`
- `BACKEND_PUBLIC_URL`
  dipakai oleh browser-facing auth bridge agar request login/refresh/logout tetap menuju URL yang bisa diakses browser user

### Contoh `.env` Minimal

Jangan commit secret riil ke repo. Gunakan placeholder seperti ini:

```env
ENV=development
DEV_HOST=localhost
DEV_PORT=8000
HOST=0.0.0.0
PORT=8000
FRONTEND_PORT=5504
WORKERS=1

DEV_DB_URL=sqlite+aiosqlite:///./app/db/campaign_data_dev.db
DB_URL=sqlite+aiosqlite:///./app/db/campaign_data.db

FRONTEND_URL=http://localhost:5504,http://127.0.0.1:5504,http://localhost:8501,http://127.0.0.1:8501
BACKEND_PUBLIC_URL=http://localhost:8000
STREAMLIT_API_HOST=http://localhost:8000

CSRF_SECRET=replace-me
JWT_SECRET_KEY=replace-me
JWT_REFRESH_SECRET_KEY=replace-me
APP_ENCRYPTION_KEY=replace-me

ACCESS_TOKEN_EXPIRE_MINUTES=60
REFRESH_TOKEN_EXPIRE_DAYS=7
ALGORITHM=HS256

BOOTSTRAP_SUPERADMIN=false
INITIAL_SUPERADMIN_NAME=admin
INITIAL_SUPERADMIN_EMAIL=admin@example.com
INITIAL_SUPERADMIN_PASSWORD=replace-me

AUTO_INIT_DB_ON_STARTUP=false
SQLITE_BUSY_TIMEOUT_MS=30000
ALLOW_CONCURRENT_ETL_RUNS=false
REQUEST_LOG_QUEUE_MAX_SIZE=1000
REQUEST_LOG_FLUSH_BATCH_SIZE=50
REQUEST_LOG_FLUSH_INTERVAL_SECONDS=1.0
```

### Streamlit Secrets

Jika dipakai, `.streamlit/secrets.toml` dapat menyediakan fallback URL backend
melalui `[api].DEV_HOST` dan `[api].HOST`. Helper runtime mendahulukan
`STREAMLIT_API_HOST` untuk server-side call, lalu `BACKEND_PUBLIC_URL`, sebelum
fallback ini. Browser auth bridge menggunakan URL publik.

## Menjalankan Secara Lokal

Siapkan `.env` lokal dari template dan isi secret/integrasi yang dibutuhkan.
Untuk contoh port 8000, selaraskan `DEV_PORT`, `PORT`, `BACKEND_PUBLIC_URL`,
`STREAMLIT_API_HOST`, dan redirect URI provider ke port tersebut. Template saat
ini memuat sebagian URL port 5505; jangan menganggap semua port template sudah
selaras. Extractor Instagram membaca `INSTAGRAM_USER_ID`.

### 1. Install dependency runtime

```bash
pip install -r requirements.txt
```

### 2. Install dependency dev

```bash
pip install -r requirements-dev.txt
```

### 3. Inisialisasi schema database

```bash
python init_db.py
```

Langkah ini wajib jika `AUTO_INIT_DB_ON_STARTUP=false`.

Untuk database existing, jalankan migration-lite yang idempotent:

```bash
python migrate_db.py
```

### 4. Jalankan backend

```bash
python main.py
```

Alternatif:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 5. Jalankan frontend

```bash
streamlit run streamlit_run.py --server.port 5504
```

### 6. Jalankan scheduler manual

```bash
python run_scheduled_etl.py
```

Contoh dengan source spesifik:

```bash
python run_scheduled_etl.py --sources google_ads ga4_daily_metrics --triggered-by manual
```

### 7. Backup SQLite manual

```bash
python scripts/backup_sqlite.py
```

### 8. Maintenance SQLite

```bash
python scripts/sqlite_maintenance.py
python scripts/sqlite_maintenance.py --backup
python scripts/sqlite_maintenance.py --vacuum
```

Maintenance helper menjalankan `PRAGMA integrity_check`, menampilkan ukuran DB,
row count per table, dan dapat membuat backup yang diverifikasi.

## Menjalankan via Docker Compose

Repo ini sudah punya `docker-compose.yml` dengan empat service:

- `db-init`
- `backend`
- `frontend`
- `scheduler`

Flow utamanya:

1. `db-init` menjalankan `python init_db.py`
2. `backend` menunggu `db-init` selesai, menjalankan `python migrate_db.py`, lalu start FastAPI
3. `frontend` menunggu backend healthy lalu start Streamlit
4. `scheduler` menunggu DB dan backend siap lalu menjalankan cron daemon

Command:

```bash
docker compose up --build
```

Port mengikuti environment Compose: backend `${PORT}`, frontend `${FRONTEND_PORT}`.
Contoh konfigurasi di atas menggunakan 8000 dan 5504. Untuk Compose, gunakan
`ENV=production` agar backend mengikuti `HOST`/`PORT` dan `DB_URL`; service Compose
menetapkan `STREAMLIT_API_HOST=http://backend:${PORT}` untuk frontend.

Volume penting yang dipersist:

- `./app/db`
- `./logs`
- `./run`
- `./backups/sqlite`

## Command Shortcut

Local command yang relevan:

```bash
python3 init_db.py
python3 migrate_db.py
python3 main.py
streamlit run streamlit_run.py --server.port 5504
python3 scripts/backup_sqlite.py
python3 scripts/sqlite_maintenance.py --backup
```

Docker command yang umum dipakai:

```bash
docker compose up -d --build
docker compose ps
docker logs tf-backend --tail 100
docker logs tf-frontend --tail 100
docker compose down
```

Untuk Docker flow, migration sudah dijalankan otomatis oleh backend container
sebelum `main.py`. Manual `python migrate_db.py` tetap tersedia untuk local run
atau operasi database di luar Docker.

## Catatan Operasional Penting

- SQLite deployment ini didesain untuk `WORKERS=1`. Jangan naikin worker backend tanpa ganti strategi database.
- `AUTO_INIT_DB_ON_STARTUP` sebaiknya tetap `false` untuk deployment yang lebih terkontrol.
- `ALLOW_CONCURRENT_ETL_RUNS=false` adalah default yang aman untuk mencegah ETL overlap.
- Streamlit server-side call bisa memakai `STREAMLIT_API_HOST`, tapi browser auth flow tetap butuh `BACKEND_PUBLIC_URL` yang benar-benar reachable dari browser.
- Pengelolaan token Google Ads, Meta Ads, Instagram, YouTube, dan TikTok memerlukan `superadmin`.
- Healthcheck backend membuktikan koneksi DB; tidak membuktikan source ETL sudah berhasil atau datanya terbaru.

## Testing dan Verifikasi

Dev dependency sudah menyediakan:

- `pytest`
- `pytest-cov`
- `ruff`

Folder `tests/` sengaja disimpan lokal dan di-ignore Git. File test tidak ikut
fresh clone; instalasi dependency dev tidak menyediakan test suite tersebut.
Jika file test tersedia di workspace lokal, jalankan:

```bash
python -m pytest -q
```

Untuk verifikasi akses Campaign, jika file lokalnya tersedia:

```bash
python -m pytest -q tests/test_campaign_access.py
```

Test tersebut memeriksa izin role pada ketiga endpoint Campaign, penolakan request
anonim, dan pembatasan Revenue untuk digital marketing. Kondisi dan cakupan test
lain bergantung pada file lokal; README tidak menyatakan seluruh suite selalu lulus.

## File Lokal dan Secret

- `.env` di-ignore dan tidak tracked pada checkout saat dokumentasi ini diperbarui.
- `.env.example` tersedia sebagai template konfigurasi; isi kredensial pada file lokal atau environment deployment.
- `tests/` dan `docs/project-understanding.md` sengaja di-ignore dan tidak tracked.
- Ignore tidak menghapus file yang pernah masuk riwayat commit. Riwayat kebocoran secret belum diaudit; keberadaan `.env` lokal bukan bukti bahwa secret pernah dipush.
- Jika kredensial diketahui pernah terekspos, lakukan rotasi pada provider terkait.

## Entry Point Penting

- API app: `main.py`
- Streamlit app: `streamlit_run.py`
- DB init: `init_db.py`
- Scheduled ETL: `run_scheduled_etl.py`
- Docker scheduler entrypoint: `docker/scheduler-entrypoint.sh`
