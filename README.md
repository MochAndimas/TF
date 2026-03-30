# Traders Family Dashboard

Dashboard internal untuk memantau performa campaign, active users, dan first deposit dengan backend FastAPI dan frontend Streamlit.

## Gambaran Umum

Project ini terdiri dari dua aplikasi utama:

- Backend FastAPI untuk authentication, analytics API, ETL trigger, dan Google Ads OAuth callback.
- Frontend Streamlit untuk login, navigasi role-based, visualisasi chart/table, dan manual update data.

Data utama yang dikelola:

- Ads data: Google Ads, Facebook Ads, TikTok Ads
- Firebase / GA4 daily metrics
- First deposit / revenue
- Account dan session metadata
- ETL run log dan request log

## Stack

- Python 3.12
- FastAPI
- SQLAlchemy async
- SQLite
- Streamlit
- Pandas
- Plotly
- Google OAuth / Google Ads auth flow

## Fitur Utama

### Authentication dan account management

- Login dengan bearer access token
- Refresh access token via `POST /api/token/refresh`
- Session restore dari cookie `HttpOnly` berisi opaque session handle
- Logout dengan revocation session
- Role-based access untuk halaman dan endpoint sensitif
- Create account dan delete account untuk `superadmin`
- Optional bootstrap one-time `superadmin` via environment variable

### Security hardening yang sudah aktif

- Access token dan refresh token tidak disimpan plaintext di database
- JWT/session token memakai `jti` dan rotation policy
- Rate limit untuk login dan token refresh
- Temporary lockout dan audit event untuk failed login
- Redaction logging untuk field sensitif seperti `password`, `access_token`, `refresh_token`, `client_secret`, dan `Authorization`
- Endpoint auth / OAuth sensitif tidak menyimpan request-response body ke tabel log
- CORS memakai explicit allowlist, bukan wildcard
- Cookie policy konsisten via `secure` dan `SameSite=lax`
- Baseline security headers:
  - `Content-Security-Policy`
  - `X-Frame-Options`
  - `X-Content-Type-Options`
  - `Referrer-Policy`
  - `Permissions-Policy`
  - `Strict-Transport-Security` hanya saat request benar-benar HTTPS

### Dashboard pages

- `Home`
- `Overview`
  - Active users App/Web
  - Campaign cost summary dan pie breakdown
  - Leads acquisition overview
  - Brand awareness overview
- `User Acquisition`
  - KPI cards per source
  - performance charts
  - insight charts
  - performance table
- `Brand Awareness`
  - KPI cards per source
  - spend chart
  - performance chart
  - insight trend charts
  - performance table
- `First Deposit`
  - daily cross-tab report
  - summary current vs previous period
- `Create Account`
- `Update Data`
- `Google Ads Token`
- `Meta Ads Token`

### ETL dan data update

- Manual ETL trigger dari UI / API
- Asynchronous ETL run tracking dengan `run_id`
- Scheduled ETL runner via `run_scheduled_etl.py`
- Shell wrapper untuk cron di `scripts/run_scheduled_etl.sh`

## Struktur Project

```text
.
|- app/
|  |- api/v1/endpoint/        # auth, campaign, deposit, overview, feature, Google Ads OAuth
|  |- api/v1/functions/       # helper payload builder endpoint layer
|  |- core/                   # config dan security utilities
|  |- db/                     # session, base, SQLAlchemy models, SQLite DB file
|  |- etl/                    # extract, transform, quality, staging, load, job runner
|  |- schemas/                # pydantic schemas
|  |- utils/                  # app bootstrap, analytics services, user/session utilities
|- streamlit_app/
|  |- functions/              # helper HTTP, cookie, chart rendering, account modal
|  |- page/                   # halaman Streamlit aktif
|- scripts/
|  |- cron/                   # source-of-truth jadwal ETL cron
|  |- run_scheduled_etl.sh    # wrapper scheduler / cron
|- docker/
|  |- scheduler-entrypoint.sh # bootstrap cron daemon di container scheduler
|- .dockerignore
|- Dockerfile
|- docker-compose.yml
|- tests/
|  |- test_etl_quality_and_upsert.py
|  |- test_security_hardening.py
|- main.py                    # entrypoint FastAPI
|- streamlit_run.py           # entrypoint Streamlit
|- run_scheduled_etl.py       # CLI scheduled ETL runner
```

## Halaman dan role access

Role yang digunakan aplikasi:

- `superadmin`
- `admin`
- `digital_marketing`
- `sales`

Hak akses halaman saat ini:

- `superadmin`: semua halaman
- `admin`: `Home`, `Overview`, `User Acquisition`, `Brand Awareness`, `First Deposit`
- `digital_marketing`: `Home`, `Overview`, `User Acquisition`, `Brand Awareness`
- `sales`: saat ini belum diaktifkan di navigasi Streamlit

## Konfigurasi Environment

Project ini masih membaca konfigurasi dari `.env`, tetapi loader config sudah support pola `*_FILE` supaya secret bisa diambil dari mounted file / secret manager.

Contoh:

- `JWT_SECRET_KEY` atau `JWT_SECRET_KEY_FILE`
- `JWT_REFRESH_SECRET_KEY` atau `JWT_REFRESH_SECRET_KEY_FILE`
- `CSRF_SECRET` atau `CSRF_SECRET_FILE`
- `APP_ENCRYPTION_KEY` atau `APP_ENCRYPTION_KEY_FILE`
- `INITIAL_SUPERADMIN_PASSWORD` atau `INITIAL_SUPERADMIN_PASSWORD_FILE`

### Variable penting backend

- `ENV`
- `DEV_HOST`, `DEV_PORT`
- `HOST`, `PORT`
- `WORKERS`
- `DEV_DB_URL`
- `DB_URL`
- `FRONTEND_URL`
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

### Variable integrasi data

- `GSHEET_SA_CREDS`
- `GSHEET_SHEET_ID`
- `GA4_PROPERTY_ID`
- `GA4_SA_CREDS`
- `USD_TO_IDR_RATE`
- `GOOGLE_ADS_CLIENT_ID`
- `GOOGLE_ADS_CLIENT_SECRET`
- `GOOGLE_ADS_REDIRECT_URI`
- `BACKEND_PUBLIC_URL`

### Streamlit secrets

File `.streamlit/secrets.toml` dipakai untuk:

- `[api]`
  - `DEV_HOST`
  - `HOST`
- `[db]`
  - `DB_DEV`
  - `DB`
- `[key]`
  - `JWT_SECRET_KEY`

### Variable tambahan untuk Docker

- `STREAMLIT_API_HOST`
  - optional override untuk server-side call dari container Streamlit ke backend internal Docker
  - contoh: `http://backend:8000`

## Menjalankan Project

### 1. Install dependency

```bash
pip install -r requirements.txt
```

### 2. Jalankan backend

```bash
python main.py
```

atau:

```bash
uvicorn main:app --host 0.0.0.0 --port 5505 --reload
```

### 3. Jalankan frontend

```bash
streamlit run streamlit_run.py --server.port 5504
```

atau:

```bash
streamlit run streamlit_run.py --server.port 5504
```

## Menjalankan Dengan Docker

Setup Docker saat ini ditujukan untuk local/dev dan memakai tiga service terpisah dengan satu image dasar yang sama:

- `backend`: FastAPI / Uvicorn
- `frontend`: Streamlit
- `scheduler`: cron daemon yang memanggil `scripts/run_scheduled_etl.sh`

File yang dipakai:

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `docker/scheduler-entrypoint.sh`
- `scripts/cron/traders_family_etl.cron`
- `scripts/run_scheduled_etl.sh`

Penyimpanan yang perlu diperhatikan:

- `.env` dipakai oleh backend dan scheduler via `env_file`
- `.streamlit/secrets.toml` di-mount ke container frontend
- SQLite tetap berupa file dan di-mount dari `./app/db` ke `/app/app/db`
- `logs/` dan `run/` di-mount supaya log ETL dan lock file tetap persisten

Menjalankan seluruh stack:

```bash
docker compose build
docker compose up -d
```

Service utama:

- backend: `http://localhost:8000`
- frontend: `http://localhost:5504`

Melihat status dan log:

```bash
docker compose ps
docker compose logs -f backend
docker compose logs -f frontend
docker compose logs -f scheduler
```

Catatan Docker:

- setup ini masih difokuskan untuk localhost / development
- frontend Streamlit tetap membaca `.streamlit/secrets.toml`
- backend memakai `TrustedHostMiddleware`; di Compose host internal Docker sudah ikut di-allow
- scheduler container tidak menjalankan ETL langsung, tetapi menyalakan cron lalu cron akan memanggil `scripts/run_scheduled_etl.sh`

## Auth Flow yang Dipakai Sekarang

Flow auth yang aktif:

1. User login ke `POST /api/login`
2. Backend mengembalikan `access_token`, `role`, `user_id`, dan `session_id`
3. Jika `remember me` aktif, backend menyimpan opaque `session_id` di cookie `HttpOnly`
4. Saat session dipulihkan, browser memanggil `POST /api/token/refresh` memakai cookie tersebut
5. Saat access token expired di sesi aktif, Streamlit me-refresh access token memakai `session_id` opaque yang tersimpan server-side
6. Logout memanggil `POST /api/logout`, merevoke session, dan menghapus cookie auth

Catatan:

- Flow utama memakai bearer token, bukan cookie auth
- CSRF tidak lagi dipakai di alur login bearer ini
- Token mentah tidak boleh disimpan plaintext di database

## Google Ads OAuth

Endpoint yang tersedia:

- `GET /api/google-ads/oauth/start`
- `GET /api/google-ads/oauth/callback`

Perilaku saat ini:

- OAuth callback tidak menampilkan refresh token mentah di HTML
- Refresh token Google Ads disimpan aman di backend storage terenkripsi
- Halaman callback hanya menampilkan status sukses / gagal
- State OAuth disimpan di server-side session middleware

## Endpoint API

### Authentication

- `POST /api/login`
- `POST /api/token/refresh`
- `POST /api/logout`
- `POST /api/register`
- `DELETE /api/delete_account/{user_id}`

### Google Ads OAuth

- `GET /api/google-ads/oauth/start`
- `GET /api/google-ads/oauth/callback`

### Campaign analytics

- `GET /api/campaign/user-acquisition`
- `GET /api/campaign/brand-awareness`

### Deposit analytics

- `GET /api/deposit/daily-report`

### Overview analytics

- `GET /api/overview/active-users`
- `GET /api/overview/campaign-cost`
- `GET /api/overview/leads-acquisition`
- `GET /api/overview/brand-awareness`

### ETL / feature update

- `POST /api/feature-data/update-external-api`
- `GET /api/feature-data/update-external-api/{run_id}`

## Contoh Request

### Login

```bash
curl -X POST "http://localhost:5505/api/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data "username=user@email.com&password=your_password"
```

### Refresh token

```bash
curl -X POST "http://localhost:5505/api/token/refresh" \
  -H "Content-Type: application/json" \
  -d '{
    "session_id": "<opaque_session_id>"
  }'
```

### User Acquisition

```bash
curl "http://localhost:5505/api/campaign/user-acquisition?start_date=2026-01-01&end_date=2026-01-31" \
  -H "Authorization: Bearer <access_token>"
```

### Deposit daily report

```bash
curl "http://localhost:5505/api/deposit/daily-report?start_date=2026-01-01&end_date=2026-01-31&campaign_type=all" \
  -H "Authorization: Bearer <access_token>"
```

### Trigger ETL update

```bash
curl -X POST "http://localhost:5505/api/feature-data/update-external-api" \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "types": "manual",
    "start_date": "2025-12-01",
    "end_date": "2025-12-31",
    "data": "tiktok_ads"
  }'
```

### Poll status ETL

```bash
curl "http://localhost:5505/api/feature-data/update-external-api/<run_id>" \
  -H "Authorization: Bearer <access_token>"
```

## ETL Notes

Layer ETL utama:

- `extract.py`
- `transform.py`
- `quality.py`
- `staging.py`
- `load.py`
- `pipelines.py`
- `job_runner.py`

Metadata ETL yang aktif:

- `etl_runs` untuk status run
- `stg_ads_raw` untuk staging ads payload

Runner terjadwal:

```bash
python run_scheduled_etl.py
```

Opsi tambahan:

- `--sources`
- `--triggered-by`
- `--fail-fast`

Wrapper cron Linux:

```cron
0 8 * * * /bin/bash /path/to/TF/scripts/run_scheduled_etl.sh
```

Catatan scheduler Docker:

- source of truth jadwal tetap di `scripts/cron/traders_family_etl.cron`
- `scripts/cron/traders_family_etl.cron` memakai placeholder `{{PROJECT_ROOT}}`, bukan path laptop / host tertentu
- `docker/scheduler-entrypoint.sh` akan merender placeholder itu ke `PROJECT_ROOT` saat container scheduler start
- default `PROJECT_ROOT` di Docker adalah `/app`, tetapi bisa dioverride via environment bila layout container berubah
- cron job di container akan me-load `/etc/environment` lebih dulu supaya `PATH` dan env lain ikut terbaca

Cek scheduler di Docker:

```bash
docker compose ps
docker compose exec scheduler cat /etc/cron.d/traders-family-etl
tail -f logs/scheduled_etl.log
```

## Testing

Menjalankan seluruh test:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

Test yang saat ini tersedia:

- `tests/test_etl_quality_and_upsert.py`
- `tests/test_security_hardening.py`

## Security dan operasional

- Jangan commit secret produksi ke `.env`
- Gunakan secret manager atau file-mounted secret untuk deployment nyata
- Semua credential/token yang pernah tersimpan plaintext sebelumnya sebaiknya di-rotate
- CORS harus memakai explicit origin saat `allow_credentials=True`
- Endpoint update ETL dan account management dibatasi ke `admin` / `superadmin`

## Bootstrap superadmin

Gunakan hanya untuk deploy pertama saat belum ada user aktif.

1. Set:
   - `BOOTSTRAP_SUPERADMIN=true`
   - `INITIAL_SUPERADMIN_NAME=<nama>`
   - `INITIAL_SUPERADMIN_EMAIL=<email>`
   - `INITIAL_SUPERADMIN_PASSWORD=<password-kuat>`
2. Jalankan backend sampai startup selesai
3. Login dengan akun tersebut
4. Kembalikan `BOOTSTRAP_SUPERADMIN=false`

## Catatan

- Database schema dibuat otomatis saat startup FastAPI
- File SQLite development saat ini ada di `app/db/campaign_data_dev.db`
- Folder `venv/` bukan source code aplikasi
