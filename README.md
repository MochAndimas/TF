# Traders Family Dashboard

Dashboard internal untuk memantau data campaign dan sinkronisasi data dari beberapa sumber (Google Ads, Facebook Ads, TikTok Ads, dan data depo), dengan backend FastAPI + frontend Streamlit.

## Ringkasan

- Backend: FastAPI (`main.py`) + SQLAlchemy async + SQLite
- Frontend: Streamlit (`streamlit_run.py`)
- Auth: JWT (access/refresh), session DB, CSRF token
- Integrasi data: Google Sheets API + endpoint JSON eksternal (depo)

## Fitur Utama

- Login/logout dengan CSRF validation
- One-time bootstrap akun `superadmin` saat startup (opsional via ENV)
- Role-based page access:
  - `superadmin`: user acquisition, create account, update data
  - `admin`, `digital_marketing`, `sales`: user acquisition
- Account management (create/edit/delete user)
- Dashboard `User Acquisition` (Streamlit):
  - Leads by Source (table + pie chart)
  - Metrics per source (Google/Facebook/TikTok) dengan growth vs previous period
- Update data manual/auto untuk:
  - Unique Campaign
  - Google Ads (GSheet)
  - Facebook Ads (GSheet)
  - TikTok Ads (GSheet)
  - Data Depo
- Request logging ke tabel `log_data`

## Struktur Project

```text
.
|- app/
|  |- api/v1/endpoint/        # endpoint auth + feature + campaign analytics
|  |- api/v1/functions/       # helper payload builder untuk endpoint campaign
|  |- core/                   # config + security
|  |- db/                     # session + models
|  |- schemas/                # pydantic schemas (auth/feature/campaign)
|  |- utils/                  # app bootstrap, user utils, gsheet utils
|- streamlit_app/
|  |- page/                   # login, user_acquisition, brand_awareness, register, update_data
|  |- functions/              # helper streamlit (cookie/session)
|- main.py                    # entrypoint FastAPI
|- streamlit_run.py           # entrypoint Streamlit
|- run_app.py                 # shortcut run Streamlit (port 5504)
```

## Kebutuhan

- Python 3.10+ (disarankan pakai virtual environment)
- Dependency sesuai `requirements.txt`
- File konfigurasi:
  - `.env`
  - `.streamlit/secrets.toml`

## Konfigurasi Environment

### 1) `.env` (backend FastAPI)

Pastikan key berikut ada:

- `ENV`
- `HOST`, `PORT`, `WORKERS`
- `DEV_DB_URL` / `DB_URL`
- `FRONTEND_URL`
- `CSRF_SECRET`
- `JWT_SECRET_KEY`
- `JWT_REFRESH_SECRET_KEY`
- `ACCESS_TOKEN_EXPIRE_MINUTES`
- `REFRESH_TOKEN_EXPIRE_DAYS`
- `ALGORITHM`
- `GSHEET_REFRESH_TOKEN`
- `GSHEET_TOKEN_URI`
- `GSHEET_CLIENT_ID`
- `GSHEET_CLIENT_SECRET`
- `GSHEET_SHEET_ID`
- `BOOTSTRAP_SUPERADMIN`
- `INITIAL_SUPERADMIN_NAME`
- `INITIAL_SUPERADMIN_EMAIL`
- `INITIAL_SUPERADMIN_PASSWORD`

### 2) `.streamlit/secrets.toml` (frontend Streamlit)

Pastikan section berikut tersedia:

- `[api]`: `DEV_HOST`, `HOST`
- `[db]`: `DB_DEV`, `DB`
- `[key]`: `JWT_SECRET_KEY`

## Cara Menjalankan

### 1) Install dependency

```bash
pip install -r requirements.txt
```

### 2) Jalankan backend FastAPI

Opsi A:
```bash
python main.py
```

Opsi B:
```bash
uvicorn main:app --host 0.0.0.0 --port 5505 --reload
```

### 3) Jalankan frontend Streamlit

Opsi A:
```bash
streamlit run streamlit_run.py --server.port 5504
```

Opsi B:
```bash
python run_app.py
```

## Endpoint API yang Dipakai Frontend

- `POST /api/login/csrf-token`
- `POST /api/login`
- `POST /api/logout`
- `POST /api/register`
- `DELETE /api/delete_account/{user_id}`
- `POST /api/feature-data/update-external-api`
- `GET /api/campaign/user-acquisition?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&chart=both`
- `GET /api/campaign/brand-awareness?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

## User Acquisition Flow

Frontend `streamlit_app/page/user_acquisition.py` menggunakan endpoint `GET /api/campaign/user-acquisition` untuk sekali request:

- `leads_by_source`:
  - table (Plotly JSON)
  - pie chart (Plotly JSON)
- `ads_metrics_with_growth`:
  - `google`
  - `facebook`
  - `tiktok`

Filter aktif:
- `Periods` (Last 7 Day, Last 30 Day, This Month, Last Month, Custom Range)
- `Performance Source` (Google Ads, Facebook Ads, TikTok Ads)

## Bootstrap Superadmin (First Deploy)

Gunakan fitur ini hanya untuk inisialisasi akun pertama saat database masih kosong.

1. Set `.env`:
   - `BOOTSTRAP_SUPERADMIN=true`
   - `INITIAL_SUPERADMIN_NAME=<nama>`
   - `INITIAL_SUPERADMIN_EMAIL=<email>`
   - `INITIAL_SUPERADMIN_PASSWORD=<password-kuat>`
2. Jalankan backend (`python main.py`) sampai startup selesai.
3. Login memakai akun tersebut.
4. Kembalikan `BOOTSTRAP_SUPERADMIN=false` dan restart service.

Perilaku bootstrap:
- Jika email superadmin sudah ada, proses create di-skip.
- Jika sudah ada user aktif lain di DB, proses create di-skip.
- Tujuan: aman dari duplikasi akun saat restart.

## Catatan

- Database akan otomatis membuat tabel saat FastAPI startup.
- Halaman register/update data hanya tersedia untuk role yang diizinkan.
- Jangan simpan kredensial bootstrap default di repo; isi dari secret manager atau env server saat deploy.
