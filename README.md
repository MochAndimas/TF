# Traders Family Dashboard

Dashboard internal untuk memantau performa campaign, overview acquisition, active users, dan first deposit. Repo ini menggabungkan FastAPI sebagai backend API, Streamlit sebagai dashboard UI, ETL pipeline untuk sinkronisasi data, serta scheduler berbasis cron untuk update harian.

## Ringkasan Arsitektur

Project ini berjalan sebagai single-node application dengan SQLite file-based storage. Arsitektur yang dipakai saat ini:

- `FastAPI` untuk authentication, authorization, analytics API, ETL trigger, OAuth callback, dan endpoint operational.
- `Streamlit` untuk login, session restore, role-based navigation, visualisasi analytics, account management, dan manual ETL trigger.
- `ETL layer` untuk ingest data campaign, GA4, dan first deposit dari external source.
- `Cron scheduler` untuk menjalankan ETL harian secara serial.
- `SQLite` sebagai primary datastore untuk user, token/session, ETL run, request log, managed secret, dan data analytics.

Karena database utama masih SQLite, ada constraint operasional yang memang disengaja:

- Backend harus dijalankan dengan `WORKERS=1`.
- ETL write tidak boleh overlap liar.
- Inisialisasi schema dibuat explicit lewat `python init_db.py` atau service `db-init`.
- Deployment diasumsikan single host, bukan multi-node shared database.

## Komponen Utama

### 1. Backend FastAPI

Backend entrypoint ada di `main.py` dan bootstrap utama ada di `app/utils/app_utils.py`.

Tanggung jawab utamanya:

- login, logout, refresh token, session restore
- account CRUD untuk `superadmin`
- endpoint analytics untuk `overview`, `campaign`, dan `deposit`
- trigger manual ETL dan polling status ETL
- Google Ads OAuth callback
- Meta Ads token exchange dan status
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
- `GET /api/deposit/daily-report`
- `POST /api/feature-data/update-external-api`
- `GET /api/feature-data/update-external-api/{run_id}`
- `GET /api/google-ads/oauth/start`
- `GET /api/google-ads/oauth/callback`
- `GET /api/meta-ads/token/status`
- `POST /api/meta-ads/token/exchange`
- `GET /health`

### 2. Frontend Streamlit

Frontend entrypoint ada di `streamlit_run.py`. UI Streamlit ini bukan sekadar dashboard chart, tapi juga shell aplikasi internal:

- login form
- browser-based auth bridge untuk cookie-aware login/refresh/logout
- restore session dari `HttpOnly` cookie
- role-based sidebar navigation
- halaman analytics
- halaman account management
- halaman manual update data
- halaman konfigurasi token Google Ads dan Meta Ads

Public page yang bisa diakses tanpa dashboard login penuh:

- `google_ads_token`
- `meta_ads_token`

### 3. ETL dan Scheduler

Alur ETL dirakit di:

- `app/etl/extract.py`
- `app/etl/transform.py`
- `app/etl/quality.py`
- `app/etl/staging.py`
- `app/etl/load.py`
- `app/etl/pipelines.py`
- `app/etl/job_runner.py`

Source ETL terjadwal default:

- `google_ads`
- `facebook_ads`
- `tiktok_ads`
- `unique_campaign`
- `ga4_daily_metrics`
- `first_deposit`

Scheduler harian:

- source cron: `scripts/cron/traders_family_etl.cron`
- wrapper: `scripts/run_scheduled_etl.sh`
- CLI runner: `run_scheduled_etl.py`
- container entrypoint: `docker/scheduler-entrypoint.sh`

Secara default cron dijalankan setiap hari pukul `08:00 Asia/Jakarta`.

## Fitur yang Sudah Ada

### Authentication dan Session

- bearer access token untuk API call
- refresh token rotation
- persistent auth session via cookie `tf_session`
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
- campaign analytics untuk `user acquisition` dan `brand awareness`
- first deposit report
- account management untuk `superadmin`
- manual ETL trigger dengan status polling
- healthcheck backend
- backup helper untuk SQLite

## Role Access

Role yang saat ini dipakai aplikasi:

- `superadmin`
- `admin`
- `digital_marketing`
- `sales`

Akses halaman Streamlit saat ini:

- `superadmin`: semua halaman
- `admin`: `home`, `overview`, `user_acquisition`, `brand_awareness`, `deposit_report`
- `digital_marketing`: `home`, `overview`, `user_acquisition`, `brand_awareness`
- `sales`: role sudah dikenal di beberapa bagian aplikasi, tetapi belum mendapat navigasi aktif di shell Streamlit

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

Project membaca konfigurasi dari `.env` lewat `python-decouple`, plus mendukung pola `*_FILE` untuk secret.

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
- `ALGORITHM`
- `BOOTSTRAP_SUPERADMIN`
- `INITIAL_SUPERADMIN_NAME`
- `INITIAL_SUPERADMIN_EMAIL`
- `INITIAL_SUPERADMIN_PASSWORD`
- `AUTO_INIT_DB_ON_STARTUP`
- `SQLITE_BUSY_TIMEOUT_MS`
- `ALLOW_CONCURRENT_ETL_RUNS`

### Variabel Integrasi External Service

- `GSHEET_SA_CREDS`
- `GSHEET_SHEET_ID`
- `FIRST_DEPOSIT_SHEET_ID`
- `FIRST_DEPOSIT_SHEET_RANGE`
- `DAILY_REGIS_SHEET_ID`
- `DAILY_REGIS_SHEET_RANGE`
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

### Variabel Tambahan untuk Streamlit dan Docker

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
WORKERS=1

DEV_DB_URL=sqlite+aiosqlite:///./app/db/campaign_data_dev.db
DB_URL=sqlite+aiosqlite:///./app/db/campaign_data.db

FRONTEND_URL=http://localhost:5504,http://127.0.0.1:5504,http://localhost:8501,http://127.0.0.1:8501
BACKEND_PUBLIC_URL=http://localhost:8000
STREAMLIT_API_HOST=http://backend:8000

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
```

### Streamlit Secrets

Jika dipakai, `.streamlit/secrets.toml` dapat menyediakan fallback untuk:

- `[api]`
  - `DEV_HOST`
  - `HOST`
- `[db]`
  - `DB_DEV`
  - `DB`
- `[key]`
  - `JWT_SECRET_KEY`

## Menjalankan Secara Lokal

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

## Menjalankan via Docker Compose

Repo ini sudah punya `docker-compose.yml` dengan empat service:

- `db-init`
- `backend`
- `frontend`
- `scheduler`

Flow utamanya:

1. `db-init` menjalankan `python init_db.py`
2. `backend` menunggu `db-init` selesai lalu start FastAPI
3. `frontend` menunggu backend healthy lalu start Streamlit
4. `scheduler` menunggu DB dan backend siap lalu menjalankan cron daemon

Command:

```bash
docker compose up --build
```

Port default:

- backend: `8000`
- frontend: `5504`

Volume penting yang dipersist:

- `./app/db`
- `./logs`
- `./run`
- `./backups/sqlite`

## Command Shortcut

Local command yang relevan:

```bash
python3 init_db.py
python3 main.py
streamlit run streamlit_run.py --server.port 5504
python3 -m pytest -q
python3 scripts/backup_sqlite.py
```

Docker command yang umum dipakai:

```bash
docker compose up -d --build
docker compose ps
docker logs tf-backend --tail 100
docker logs tf-frontend --tail 100
docker compose down
```

## Catatan Operasional Penting

- SQLite deployment ini didesain untuk `WORKERS=1`. Jangan naikin worker backend tanpa ganti strategi database.
- `AUTO_INIT_DB_ON_STARTUP` sebaiknya tetap `false` untuk deployment yang lebih terkontrol.
- `ALLOW_CONCURRENT_ETL_RUNS=false` adalah default yang aman untuk mencegah ETL overlap.
- Streamlit server-side call bisa memakai `STREAMLIT_API_HOST`, tapi browser auth flow tetap butuh `BACKEND_PUBLIC_URL` yang benar-benar reachable dari browser.
- Google Ads OAuth dan Meta token exchange hanya relevan untuk role `superadmin`.

## Testing dan Verifikasi

Dev dependency sudah menyediakan:

- `pytest`
- `pytest-cov`
- `ruff`

Namun kondisi repo saat ini perlu dicatat:

- folder `tests/` saat ini tidak lagi berisi file test `.py`, hanya artefak `__pycache__`
- environment aktif tempat audit ini dijalankan belum memiliki `pytest` terinstall

Jika ingin mengaktifkan workflow test lagi:

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

## Temuan Audit Repo

Berikut poin penting dari hasil pengecekan repo saat ini:

- arsitektur project sudah cukup jelas dan modular antara backend, Streamlit, ETL, dan scheduler
- deployment Docker sudah disusun sesuai constraint SQLite single-worker
- flow auth lebih matang dibanding README lama, termasuk session restore, refresh rotation, dan auth bridge browser-side
- ada dukungan secret encryption untuk token eksternal dan support `*_FILE` untuk secret injection
- scheduler ETL sudah punya locking via `flock` atau lock file fallback
- dokumentasi sebelumnya belum cukup menonjolkan risiko besar: `.env` di repo saat ini berisi secret sensitif dan sebaiknya segera diganti serta di-rotate di provider terkait
- status testing sekarang tidak sepenuhnya sehat dari sisi repository hygiene karena file test sumber sudah hilang dari `tests/`

## Rekomendasi Lanjutan

- rotate semua secret yang pernah tersimpan di `.env` repo ini
- sediakan `.env.example` yang steril dari secret riil
- kembalikan file test sumber ke `tests/` agar `python3 -m pytest -q` punya coverage nyata
- pertimbangkan migrasi ke PostgreSQL jika concurrency dan deployment mulai berkembang

## Entry Point Penting

- API app: `main.py`
- Streamlit app: `streamlit_run.py`
- DB init: `init_db.py`
- Scheduled ETL: `run_scheduled_etl.py`
- Docker scheduler entrypoint: `docker/scheduler-entrypoint.sh`
