# Traders Family Dashboard

Dashboard internal untuk memantau performa campaign dan revenue deposit, dengan backend FastAPI dan frontend Streamlit.

## Ringkasan

- Backend: FastAPI (`main.py`) + SQLAlchemy async + SQLite
- Frontend: Streamlit (`streamlit_run.py`)
- Auth: JWT access token + session token table + CSRF validation
- Integrasi data: Google Sheets (campaign ads) + GA4 Analytics Data API
- Logging: request/response API disimpan ke tabel `log_data`
- ETL update external API sudah asynchronous job-based (`run_id` + status polling)

## Fitur Utama

- Login/logout dengan CSRF protection
- Bootstrap one-time akun `superadmin` via ENV (opsional)
- Role-based navigation di Streamlit:
  - `superadmin`: Overall, User Acquisition, Brand Awareness, First Deposit report, Create Account, Update Data
  - `admin`, `digital_marketing`, `sales`: Overall, User Acquisition, Brand Awareness, First Deposit report
- Dashboard `Overall`
  - FireBase Active User (source App/Web) + stickiness metrics
  - Ad Cost Spend (total/google/facebook/tiktok) + 3 pie breakdown charts
  - Overall Performance Campaign User Acquisition
    - metrics + growth (cost, impressions, clicks, leads, cost/leads, first deposit, cost to first deposit)
    - leads by source (table + pie), cost vs leads, leads per day, cost to first deposit
  - Overall Performance Campaign Brand Awareness
    - metrics + growth (cost, impression, clicks, CTR, CPM, CPC)
    - spend chart + performance chart
- Dashboard `User Acquisition`
  - KPI cards per source + growth vs previous period
  - performance charts, insight charts, pacing charts
  - performance table (campaign/ad group/ad name)
- Dashboard `Brand Awareness`
  - KPI cards per source + growth vs previous period
  - spend/performance charts
  - insight trend charts CTR/CPM/CPC
  - performance table
- Dashboard `First Deposit report` (Revenue/Deposit)
  - tabel cross-tab harian berdasarkan `tanggal_regis`
  - kolom per tanggal dengan subkolom `New`/`Existing`
  - section `TOTAL` + per `campaign_id`
  - summary cards `New User`/`Existing User` di section `First Deposit Summary` dengan growth vs previous period
- Update data manual dari endpoint update external API

## Struktur Project

```text
.
|- app/
|  |- api/v1/endpoint/        # auth, feature update, campaign, deposit, overview
|  |- api/v1/functions/       # payload builder untuk campaign/deposit/overview
|  |- core/                   # config + security helpers
|  |- db/                     # session + SQLAlchemy models
|  |- etl/                    # extract, transform, load, staging, quality, pipelines
|  |- schemas/                # pydantic schemas
|  |- utils/                  # app bootstrap + business/service utils (termasuk overview_utils)
|- streamlit_app/
|  |- page/                   # login, overall/overview, UA, BA, deposit, register, update_data
|  |- functions/              # helper streamlit (cookie/session/request)
|- main.py                    # entrypoint FastAPI
|- streamlit_run.py           # entrypoint Streamlit dashboard
|- run_app.py                 # shortcut run Streamlit
|- tests/                     # unit/integration tests ETL
```

## Kebutuhan

- Python 3.10+
- Dependency sesuai `requirements.txt`
- File konfigurasi:
  - `.env`
  - `.streamlit/secrets.toml`

## Konfigurasi Environment

### 1) `.env` (FastAPI)

Minimal key yang digunakan:

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
- `GSHEET_SA_CREDS` (JSON string service account Google Sheets)
- `GSHEET_SHEET_ID`
- `GA4_PROPERTY_ID`
- `GA4_REFRESH_TOKEN`
- `GA4_TOKEN_URI`
- `GA4_CLIENT_ID`
- `GA4_CLIENT_SECRET`
- `USD_TO_IDR_RATE` (opsional, default `16000`, dipakai untuk konversi First Deposit USD -> IDR di overview UA)
- `BOOTSTRAP_SUPERADMIN`
- `INITIAL_SUPERADMIN_NAME`
- `INITIAL_SUPERADMIN_EMAIL`
- `INITIAL_SUPERADMIN_PASSWORD`

### 2) `.streamlit/secrets.toml` (Streamlit)

- `[api]`: `DEV_HOST`, `HOST`
- `[db]`: `DB_DEV`, `DB`
- `[key]`: `JWT_SECRET_KEY`

## Cara Menjalankan

### 1) Install dependency

```bash
pip install -r requirements.txt
```

### 2) Jalankan backend

```bash
python main.py
```

atau

```bash
uvicorn main:app --host 0.0.0.0 --port 5505 --reload
```

### 3) Jalankan frontend

```bash
streamlit run streamlit_run.py --server.port 5504
```

atau

```bash
python run_app.py
```

## Endpoint API yang Dipakai Frontend

### Authentication

- `POST /api/login/csrf-token`
- `POST /api/login`
- `POST /api/logout`
- `POST /api/register`
- `DELETE /api/delete_account/{user_id}`

### Data Update

- `POST /api/feature-data/update-external-api`
- `GET /api/feature-data/update-external-api/{run_id}`

### Campaign Analytics

- `GET /api/campaign/user-acquisition?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `GET /api/campaign/brand-awareness?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

### Deposit Analytics

- `GET /api/deposit/daily-report?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&campaign_type=all|user_acquisition|brand_awareness`

### Overall / Overview Analytics

- `GET /api/overview/active-users?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&source=app|web`
- `GET /api/overview/campaign-cost?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `GET /api/overview/leads-acquisition?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
- `GET /api/overview/brand-awareness?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

## Contoh Request API

### Login Flow (CSRF + Login)

```bash
curl -X POST "http://localhost:5505/api/login/csrf-token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data "username=user@email.com&password=your_password"
```

```bash
curl -X POST "http://localhost:5505/api/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -H "X-CSRF-Token: <csrf_token>" \
  --data "username=user@email.com&password=your_password"
```

### User Acquisition

```bash
curl "http://localhost:5505/api/campaign/user-acquisition?start_date=2026-01-01&end_date=2026-01-31" \
  -H "Authorization: Bearer <access_token>"
```

### Brand Awareness

```bash
curl "http://localhost:5505/api/campaign/brand-awareness?start_date=2026-01-01&end_date=2026-01-31" \
  -H "Authorization: Bearer <access_token>"
```

### Deposit Daily Report

```bash
curl "http://localhost:5505/api/deposit/daily-report?start_date=2026-01-01&end_date=2026-01-31&campaign_type=all" \
  -H "Authorization: Bearer <access_token>"
```

### Update External API (Async Job)

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

```bash
curl "http://localhost:5505/api/feature-data/update-external-api/<run_id>" \
  -H "Authorization: Bearer <access_token>"
```

## ETL Runtime Notes

- ETL update flow dipisah jadi layer:
- `extract.py`: ambil data dari Google Sheets/GA4 API
  - `transform.py`: parsing + normalisasi data
  - `quality.py`: data quality checks
  - `staging.py`: simpan raw payload ke tabel staging
  - `load.py`: upsert ke tabel final
  - `pipelines.py`: orchestration flow per source
- Source tambahan: `ga4_daily_metrics` (dimension: `date`, source app/web; metrics: DAU, MAU, active users)
- Untuk `tiktok_ads`, rows dengan key sama akan di-aggregate dulu per:
  - `date, campaign_id, ad_group, ad_name`
- Untuk `google_ads`/`facebook_ads`, flow tetap tanpa agregasi khusus.

## Tabel Baru Terkait ETL

- `etl_runs`: metadata run (`run_id`, source, status, message, error, window, timestamps)
- `stg_ads_raw`: raw staging payload ads

Catatan: tabel baru dibuat saat backend startup (`create_all`) di DB aktif (`DEV_DB_URL` saat development).

## Menjalankan Test

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Smoke Test Cepat ETL

1. Trigger update via UI/POST endpoint.
2. Pastikan response berisi `run_id`.
3. Poll endpoint status sampai `success` atau `failed`.
4. Verifikasi data masuk ke tabel target + `etl_runs` ter-update.

## Payload Ringkas Dashboard

### User Acquisition (`/api/campaign/user-acquisition`)

- `ads_metrics_with_growth`
- `leads_performance_charts`
- `ads_campaign_details`
- `ua_insight_charts`

### Brand Awareness (`/api/campaign/brand-awareness`)

- `metrics_with_growth`
- `charts`
- `details`
- `insight_charts`

### Deposit Report (`/api/deposit/daily-report`)

- `start_date`, `end_date`
- `report.timeline`
- `report.sections` (`TOTAL` + per campaign)
- `report.summary` (current/previous period + growth)

### Overview Active Users (`/api/overview/active-users`)

- `start_date`, `end_date`, `source`
- `stickiness_with_growth` (last day stickiness, average stickiness, active user)
- `active_users_chart` (`figure`, `rows`)

### Overview Campaign Cost (`/api/overview/campaign-cost`)

- `cost_metrics_with_growth` (total/google/facebook/tiktok ad cost)
- `cost_breakdown_charts`
  - `cost_by_campaign_type`
  - `ua_cost_by_platform`
  - `ba_cost_by_platform`

### Overview Leads Acquisition (`/api/overview/leads-acquisition`)

- `metrics_with_growth` (cost, impressions, clicks, leads, cost_leads, first_deposit, cost_to_first_deposit)
- `leads_by_source` (table + pie chart)
- `cost_vs_leads_chart`
- `leads_per_day_chart`
- `cost_to_revenue_chart` (ditampilkan sebagai Cost To First Deposit)

### Overview Brand Awareness (`/api/overview/brand-awareness`)

- `metrics_with_growth` (cost, impressions, clicks, ctr, cpm, cpc)
- `spend_chart`
- `performance_chart`

## Contoh Response JSON (Ringkas)

### Login (`POST /api/login`)

```json
{
  "access_token": "<jwt_access_token>",
  "token_type": "Bearer",
  "role": "admin",
  "success": true
}
```

### User Acquisition (`GET /api/campaign/user-acquisition`)

```json
{
  "success": true,
  "message": "User acquisition overview generated.",
  "data": {
    "ads_metrics_with_growth": {
      "google": {
        "current_period": { "from_date": "2026-01-01", "to_date": "2026-01-31", "metrics": {} },
        "previous_period": { "from_date": "2025-12-01", "to_date": "2025-12-31", "metrics": {} },
        "growth_percentage": {}
      },
      "facebook": {},
      "tiktok": {}
    },
    "leads_performance_charts": {
      "google": {
        "cost_to_leads": { "figure": {}, "rows": [] },
        "leads_by_periods": { "figure": {}, "rows": [] },
        "clicks_to_leads": { "figure": {}, "rows": [] }
      }
    },
    "ads_campaign_details": { "google": { "rows": [] } },
    "ua_insight_charts": {
      "ratio_trends": {},
      "spend_vs_leads": {},
      "top_leads": {},
      "cumulative": {},
      "daily_mix": {}
    }
  }
}
```

### Brand Awareness (`GET /api/campaign/brand-awareness`)

```json
{
  "success": true,
  "message": "Brand awareness overview generated.",
  "data": {
    "metrics_with_growth": {
      "google": {
        "current_period": { "metrics": {} },
        "previous_period": { "metrics": {} },
        "growth_percentage": {}
      },
      "facebook": {},
      "tiktok": {}
    },
    "charts": {
      "google": {
        "spend": { "figure": {}, "rows": [] },
        "performance": { "figure": {}, "rows": [] }
      }
    },
    "details": { "google": { "rows": [] } },
    "insight_charts": {
      "ratio_trends": {
        "google": {
          "campaign_id": {
            "ctr": { "figure": {}, "rows": [] },
            "cpm": { "figure": {}, "rows": [] },
            "cpc": { "figure": {}, "rows": [] }
          }
        }
      },
      "ratio_top_n": 8
    }
  }
}
```

### Deposit Report (`GET /api/deposit/daily-report`)

```json
{
  "success": true,
  "message": "Deposit daily report generated.",
  "data": {
    "start_date": "2026-01-01",
    "end_date": "2026-01-31",
    "report": {
      "timeline": ["2026-01-01", "2026-01-02"],
      "campaign_type": "all",
      "summary": {
        "current_period": {
          "from_date": "2026-01-01",
          "to_date": "2026-01-31",
          "totals": {
            "new": { "depo_amount": 20939.59, "qty": 34, "aov": 615.87 },
            "existing": { "depo_amount": 37785.94, "qty": 7, "aov": 5397.99 }
          }
        },
        "previous_period": {
          "from_date": "2025-12-01",
          "to_date": "2025-12-31",
          "totals": {
            "new": { "depo_amount": 7540.1, "qty": 16, "aov": 471.26 },
            "existing": { "depo_amount": 1814.35, "qty": 3, "aov": 604.78 }
          }
        },
        "growth_percentage": {
          "new": { "depo_amount": 177.71, "qty": 112.5, "aov": 30.69 },
          "existing": { "depo_amount": 1982.04, "qty": 133.33, "aov": 792.57 }
        }
      },
      "sections": [
        {
          "title": "TOTAL",
          "campaign_id": "TOTAL",
          "rows": [
            { "metric": "Depo Amount ($)", "key": "depo_amount", "values": {} },
            { "metric": "Jumlah Depo (Qty)", "key": "qty", "values": {} },
            { "metric": "AOV ($)", "key": "aov", "values": {} }
          ]
        }
      ]
    }
  }
}
```

## Detail Per Page (Frontend)

### First Deposit > First Deposit report

- Filter:
  - `Periods`: Last 7 Day, Last 30 Day, This Month, Last Month, Custom Range
  - `Campaign Type`: All, User Acquisition, Brand Awareness
- Perilaku:
  - `Select Date Range` hanya muncul saat `Custom Range`
  - data auto-fetch saat filter berubah (tanpa tombol apply)
- Komponen:
  - `First Deposit Summary` cards (New User / Existing User)
  - growth di card: compare ke previous period dengan durasi sama
  - tabel cross-tab harian (kolom = tanggal, subkolom = New/Existing)

### Overall > Overview

- Filter:
  - `Periods` (preset + custom range)
- Komponen utama:
  - FireBase Active User: source selector App/Web, 3 KPI card, line dual-axis
  - Ad Cost Spend: 4 KPI card + 3 pie charts
  - User Acquisition: 7 KPI card, leads by source table+pie, 3 chart performa
  - Brand Awareness: 6 KPI card + spend/performance chart

### Campaign > User Acquisition

- Filter:
  - `Periods` + `Performance Source`
- Komponen utama:
  - KPI cards
  - 3 performance charts
  - insight charts
  - performance table dengan format:
    - `Click->Leads %` pakai suffix `%`
    - `Cost/Leads` pakai prefix `Rp` + separator ribuan

### Campaign > Brand Awareness

- Filter:
  - `Periods` + `Performance Source`
- Komponen utama:
  - KPI cards
  - spend/performance charts
  - 3 insight trend charts (`CTR`, `CPM`, `CPC`) by selected level
  - performance table

## Troubleshooting Cepat

- `401 Unauthorized`:
  - pastikan alur login + csrf-token sudah benar
  - access token belum expired
- Data kosong di chart/table:
  - cek range tanggal
  - cek data source sudah diupdate dari `Update Data`
- Growth terlihat tidak sesuai:
  - UA/BA/Deposit cards memakai compare ke `previous period` (durasi sama)
- CORS/Network error di Streamlit:
  - cek `DEV_HOST`/`HOST` di `.streamlit/secrets.toml`
  - cek backend running di host/port yang sesuai

## Catatan UI Filter

- Filter periode berjalan realtime (tanpa tombol apply).
- `Select Date Range` hanya muncul saat `Periods = Custom Range`.
- Untuk preset (`Last 7 Day`, `Last 30 Day`, dll), range langsung diisi otomatis dari preset.

## Bootstrap Superadmin (First Deploy)

Gunakan hanya saat inisialisasi pertama ketika DB belum berisi user aktif.

1. Set `.env`:
   - `BOOTSTRAP_SUPERADMIN=true`
   - `INITIAL_SUPERADMIN_NAME=<nama>`
   - `INITIAL_SUPERADMIN_EMAIL=<email>`
   - `INITIAL_SUPERADMIN_PASSWORD=<password-kuat>`
2. Jalankan backend sampai startup selesai.
3. Login dengan akun tersebut.
4. Kembalikan `BOOTSTRAP_SUPERADMIN=false`, lalu restart service.

## Catatan Tambahan

- Tabel database dibuat otomatis saat startup FastAPI.
- Kredensial produksi sebaiknya lewat secret manager, bukan hardcoded di repo.
- Folder `lib/` adalah dependency environment, bukan source aplikasi.
- Google Sheets sekarang memakai service account via `GSHEET_SA_CREDS`; spreadsheet target harus di-share ke email service account tersebut.

## Daily ETL Scheduler (Cron)

- Scheduler entrypoint untuk Linux ada di `scripts/run_scheduled_etl.sh`
- Runner Python yang dipanggil wrapper ada di `run_scheduled_etl.py`
- Log default ditulis ke `logs/scheduled_etl.log`
- Lock file default ada di `run/scheduled_etl.lock`
- Urutan source default:
  - `google_ads`
  - `facebook_ads`
  - `tiktok_ads`
  - `unique_campaign`
  - `ga4_daily_metrics`
  - `first_deposit`

Contoh crontab jam `08:00` setiap hari:

```cron
0 8 * * * /bin/bash /path/to/TF/scripts/run_scheduled_etl.sh
```

Ganti `/path/to/TF` dengan absolute path project di server Linux.

Catatan production:
- Wrapper memakai `flock` bila tersedia untuk mencegah job overlap.
- Jika `flock` tidak tersedia, wrapper fallback ke lock file sederhana.
- Kalau timezone server bukan WIB, bisa tambahkan `CRON_TZ=Asia/Jakarta` di crontab.
