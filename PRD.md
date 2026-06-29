# PRD: Scalability & Maintainability Improvement

## 1. Ringkasan

Project Traders Family Dashboard adalah aplikasi internal untuk monitoring performa campaign, acquisition, activity, dan deposit. Stack yang dipertahankan:

- Frontend: Streamlit
- Backend: FastAPI
- Database: SQLite
- Runtime: single-node deployment

Dokumen ini berisi roadmap improvement agar project tetap scalable dan maintainable tanpa mengganti stack utama. Fokus scalability di sini bukan horizontal scaling multi-node, tetapi kemampuan aplikasi menangani pertumbuhan data, endpoint, halaman, integrasi external API, dan kebutuhan operasional secara stabil di satu host.

## 2. Masalah Utama yang Mau Diselesaikan

### 2.1 Maintainability

Saat ini project sudah punya pemisahan folder yang cukup jelas antara `app/`, `streamlit_app/`, ETL, scheduler, dan tests. Namun seiring fitur bertambah, beberapa risiko maintainability perlu dikontrol:

- Pola endpoint, service, schema, dan repository belum seluruhnya konsisten.
- Logic analytics tersebar antara endpoint functions, utils, repository, dan Streamlit page.
- Banyak flow external integration yang butuh contract jelas agar perubahan API vendor tidak merusak dashboard.
- Schema SQLite masih dimaintain dengan bootstrap/additive maintenance manual, belum berbasis migrasi yang rapi.
- Testing sudah ada, tapi belum merata untuk auth full flow, endpoint integration, ETL lifecycle, dan Streamlit helper behavior.

### 2.2 Scalability dengan SQLite

SQLite tetap cocok untuk single-node internal dashboard, tetapi perlu guardrail:

- Backend harus tetap `WORKERS=1`.
- Write operation, terutama ETL, harus tetap serial atau terkunci.
- Query analytics perlu indexing, caching, pagination, dan date-window discipline.
- Request log, audit table, ETL run log, dan data historis perlu retention supaya DB tidak membengkak.
- Backup, restore, vacuum, dan integrity check harus jadi operasional rutin.

## 3. Goals

- Membuat codebase lebih modular, mudah dites, dan mudah ditambah fitur baru.
- Menjaga runtime tetap stabil dengan SQLite file-based storage.
- Mengurangi risiko data corruption, ETL overlap, dan query lambat.
- Membuat observability cukup jelas untuk debug issue produksi.
- Memperjelas kontrak antara Streamlit FE dan FastAPI BE.
- Mempercepat onboarding developer baru lewat struktur, dokumentasi, dan testing.

## 4. Non-Goals

- Tidak migrasi frontend dari Streamlit.
- Tidak migrasi backend dari FastAPI.
- Tidak migrasi database utama dari SQLite.
- Tidak membangun multi-node deployment.
- Tidak mengubah dashboard menjadi public SaaS.
- Tidak melakukan rewrite besar-besaran tanpa kebutuhan fitur.

## 5. Target User

- Internal business user yang melihat dashboard harian.
- Superadmin yang mengelola akun, token integrasi, dan manual update data.
- Analyst, marketing, dan finance yang membutuhkan data sesuai role.
- Role `sales` tetap dikenal sistem, tetapi belum perlu akses aktif dalam roadmap terdekat.
- Developer/operator yang menjaga ETL, deployment, dan kualitas data.

## 6. Prinsip Produk dan Teknis

- Streamlit tetap menjadi shell aplikasi internal, bukan landing page.
- FastAPI menjadi satu-satunya API contract untuk data, auth, ETL trigger, dan integration status.
- SQLite diperlakukan sebagai production datastore single-node dengan batasan eksplisit.
- ETL harus idempotent, observable, dan bisa diulang untuk date window tertentu.
- Business logic analytics sebaiknya berada di backend, bukan tersebar di UI.
- UI Streamlit fokus pada rendering, filtering, session state, dan user workflow.
- Semua fitur baru harus punya acceptance criteria dan minimal regression test.

## 7. Success Metrics

- Test suite bisa dijalankan lokal dengan satu command dan stabil.
- Endpoint analytics utama punya integration test.
- Query dashboard untuk date range umum selesai di bawah 2 detik pada dataset normal.
- Manual ETL tidak bisa overlap kecuali explicit config mengizinkan.
- Backup SQLite otomatis tersedia dan restore procedure terdokumentasi.
- Jumlah logic data transformation di Streamlit page berkurang secara bertahap.
- Developer bisa menambah satu dashboard page baru dengan pola yang jelas.
- Query analytics di atas 2 detik tercatat sebagai slow query.

## 8. Scope Improvement

### Epic 1: Arsitektur Backend yang Lebih Konsisten

Tujuan:

Merapikan boundary backend agar endpoint, schema, service, repository, dan query builder punya tanggung jawab yang jelas.

Rekomendasi:

- Standarkan pola per domain:
  - `endpoint`: HTTP routing, dependency, role guard.
  - `schemas`: request dan response model.
  - `services`: orchestration use case.
  - `repositories`: DB query dan persistence.
  - `utils`: helper kecil yang tidak punya state domain besar.
- Pindahkan logic payload builder yang kompleks dari `app/api/v1/functions/` ke service/repository domain secara bertahap.
- Terapkan response schema untuk endpoint analytics penting.
- Tambahkan API error format standar:
  - `code`
  - `message`
  - `detail`
  - `request_id` bila tersedia.

Acceptance Criteria:

- Endpoint baru mengikuti struktur domain yang sama.
- Minimal 3 endpoint analytics utama punya response schema eksplisit.
- Error response untuk auth dan analytics memakai format standar.
- Tidak ada breaking change pada Streamlit page existing.

Priority: High

### Epic 2: Contract Layer antara Streamlit dan FastAPI

Tujuan:

Mengurangi coupling FE-BE dan membuat Streamlit lebih mudah dirawat.

Rekomendasi:

- Buat API client wrapper per domain di `streamlit_app/functions/` atau `streamlit_app/api_clients/`.
- Standarkan return shape dari FE API call:
  - `ok`
  - `data`
  - `message`
  - `status_code`
- Hindari parsing error string langsung di page.
- Pusatkan auth refresh, timeout, retry ringan, dan logout behavior di satu client.
- Tambahkan typed helper untuk filter tanggal, pagination, dan query params.

Acceptance Criteria:

- Minimal halaman `overview`, `campaign`, dan `deposit` memakai API client pattern yang sama.
- Error UI konsisten untuk timeout, unauthorized, forbidden, dan server error.
- Tidak ada page yang membangun URL API secara manual di luar wrapper.

Priority: High

### Epic 3: SQLite Production Hardening

Tujuan:

Menjaga SQLite tetap aman dan cepat untuk single-node deployment.

Rekomendasi:

- Pertahankan `WORKERS=1` sebagai hard constraint.
- Dokumentasikan batas operasional SQLite:
  - satu host
  - satu backend worker
  - ETL write serial
  - backup via SQLite online backup API
- Tambahkan maintenance command:
  - `PRAGMA integrity_check`
  - `VACUUM` terjadwal bila dibutuhkan
  - retention cleanup
  - backup verification
- Terapkan retention policy:
  - analytics aggregate: 24 bulan
  - revenue, deposit, dan acquisition: 36 bulan
  - media/post-level insight detail: 12 bulan
  - request log: 30-90 hari
  - ETL run log: 180 hari
  - auth audit log: 12 bulan
- Tambahkan index untuk query analytics yang sering dipakai:
  - date column
  - campaign/source/platform
  - account/channel
  - foreign key/session token lookup
- Evaluasi ukuran DB dan row count per table di health/admin endpoint internal.

Acceptance Criteria:

- Ada script maintenance SQLite yang bisa dijalankan manual.
- Ada dokumentasi restore backup.
- Health/admin internal dapat menunjukkan status DB dasar tanpa expose secret.
- Query analytics utama memakai index yang relevan.
- Retention cleanup dapat dijalankan terkontrol dan tidak menghapus data di luar policy.

Priority: High

### Epic 4: Schema Migration yang Terkontrol

Tujuan:

Mengurangi risiko perubahan schema manual di SQLite.

Rekomendasi:

- Introduce migration-lite untuk SQLite.
- Pilihan konservatif:
  - tetap pakai SQLAlchemy metadata untuk create awal.
  - tambah tabel `schema_migration`.
  - setiap migration punya id, description, applied_at.
  - migration bersifat additive dan idempotent.
- Hindari destructive migration otomatis di startup.
- Pisahkan `init_db.py` untuk fresh database dan `migrate_db.py` untuk database existing.

Acceptance Criteria:

- Ada tabel migration history.
- Schema maintenance tidak lagi bergantung penuh pada pengecekan kolom tersebar.
- Migration bisa dijalankan ulang tanpa error.
- README menjelaskan kapan pakai init dan kapan pakai migrate.

Priority: High

### Epic 5: ETL Reliability dan Observability

Tujuan:

Membuat ETL lebih aman untuk dijalankan manual maupun scheduler.

Rekomendasi:

- Pastikan setiap source ETL punya:
  - run id
  - source
  - mode
  - window start/end
  - status
  - started_at
  - finished_at
  - row extracted
  - row loaded
  - error detail ringkas
- Tambahkan per-source timeout dan retry policy untuk external API.
- Tambahkan idempotency rule per source dan date window.
- Tambahkan ETL summary endpoint untuk Streamlit.
- Manual ETL tetap single-source per run. Multi-source manual trigger belum diperlukan.
- Tambahkan data quality report:
  - missing required field
  - duplicate natural key
  - invalid date
  - negative metric yang tidak valid
- Pisahkan extract, transform, quality, load contract dengan typed payload/dataclass bila memungkinkan.

Acceptance Criteria:

- Manual update data menampilkan progress/status yang mudah dipahami.
- ETL run gagal tetap tercatat dan tidak meninggalkan status menggantung.
- Scheduler tidak menjalankan run overlap.
- Manual ETL tidak menyediakan multi-source trigger di fase ini.
- Minimal 3 source ETL punya quality check teruji.

Priority: High

### Epic 6: Analytics Performance

Tujuan:

Menjaga dashboard tetap cepat saat data bertambah.

Rekomendasi:

- Pakai date range default yang bounded untuk semua analytics page.
- Tambahkan pagination atau limit untuk tabel detail besar.
- Pertahankan cache dataframe backend dengan TTL, tapi beri invalidation jelas setelah ETL sukses.
- Pertimbangkan materialized summary table di SQLite untuk metric harian:
  - campaign_daily_summary
  - acquisition_daily_summary
  - deposit_daily_summary
  - social_media_daily_summary
- Summary table direbuild saat ETL selesai, bukan dihitung ulang setiap request berat.
- Tambahkan query timing log untuk endpoint analytics.
- Terapkan threshold observability:
  - `> 2 detik`: slow query dan masuk warning log.
  - `> 5 detik`: very slow query dan masuk slow-query report/admin status.
  - `> 10 detik`: critical query dan perlu investigasi operasional.
  - `30-60 detik`: timeout target endpoint analytics, tergantung jenis laporan.
- Tambahkan admin/status page untuk DB status, backup status, ETL history, dan slow-query report.

Acceptance Criteria:

- Endpoint analytics utama punya batas date range default.
- Tabel detail besar tidak load unlimited row.
- ETL sukses meng-invalidate cache terkait.
- Query di atas 2 detik tercatat di log.
- Query di atas 5 detik tampil di admin/status page.
- Query di atas 10 detik ditandai critical untuk investigasi.
- Admin/status page menampilkan DB status, backup status, ETL history, dan slow-query report tanpa expose secret.

Priority: Medium-High

### Epic 7: Role-Based Access Control yang Lebih Mudah Dirawat

Tujuan:

Membuat role dan permission lebih eksplisit serta tidak mudah drift antara FE dan BE.

Rekomendasi:

- Buat permission registry terpusat di backend.
- Streamlit mengambil menu berdasarkan endpoint permission atau shared config yang jelas.
- Hindari role hardcoded tersebar di banyak file.
- Tambahkan test matrix role-page dan role-endpoint.
- Pertahankan role `sales` tanpa akses aktif sampai kebutuhan produk berikutnya jelas.

Acceptance Criteria:

- FE navigation dan BE endpoint guard tidak kontradiktif.
- Ada test untuk setiap role utama.
- Superadmin bisa melihat semua menu yang memang diizinkan.
- Role `sales` tidak mendapat navigasi aktif dan menampilkan pesan yang jelas bila login.

Priority: Medium-High

### Epic 8: Testing Strategy

Tujuan:

Meningkatkan confidence saat refactor dan tambah fitur.

Rekomendasi test:

- Unit test:
  - auth service
  - role guard
  - ETL transform
  - analytics calculation
  - SQLite migration helper
- Integration test:
  - login, refresh, logout
  - endpoint analytics dengan test DB
  - ETL run lifecycle
  - request logging worker
- Contract test:
  - response shape endpoint yang dipakai Streamlit.
  - error shape.
- Smoke test:
  - app import
  - database readiness
  - health endpoint.

Acceptance Criteria:

- `python -m pytest -q` menjadi command verifikasi utama.
- Test tidak bergantung pada secret external provider.
- External API test memakai fixture/mock.
- Coverage minimal ada untuk auth, ETL core, analytics payload, dan DB maintenance.

Priority: High

### Epic 9: Configuration dan Secret Hygiene

Tujuan:

Membuat konfigurasi aman, eksplisit, dan mudah dipindah environment.

Rekomendasi:

- Tambahkan `.env.example` steril tanpa secret.
- Pastikan `.env` tidak dicommit.
- Rotate secret yang pernah masuk repo atau pernah dibagikan.
- Validasi required env saat startup dengan pesan error yang actionable.
- Kelompokkan env:
  - core app
  - auth/security
  - database
  - external API
  - Streamlit
  - scheduler
  - logging/cache.
- Pertahankan support `*_FILE`.

Acceptance Criteria:

- Developer baru bisa setup dari `.env.example`.
- Startup gagal cepat bila env penting kosong.
- Tidak ada secret riil di dokumentasi.
- Checklist secret rotation tersedia.

Priority: High

### Epic 10: Developer Experience dan Code Quality

Tujuan:

Membuat project lebih enak dikembangkan tanpa menambah kompleksitas besar.

Rekomendasi:

- Tambahkan command documented untuk:
  - format/lint
  - test
  - run backend
  - run frontend
  - init/migrate DB
  - backup/restore
- Gunakan `ruff` secara konsisten.
- Tambahkan pre-commit opsional.
- Tambahkan architecture decision record ringan di `docs/adr/`.
- Tambahkan module README untuk area kompleks seperti ETL dan auth.

Acceptance Criteria:

- README punya quickstart yang benar-benar bisa diikuti.
- Ada checklist sebelum merge.
- Lint dan test command terdokumentasi.
- Area ETL punya dokumentasi data flow.

Priority: Medium

## 9. Recommended Roadmap

### Phase 1: Stabilize Foundation

Status: Completed

Estimasi: 1-2 minggu

- Tambah `.env.example`.
- Tambah SQLite maintenance script.
- Tambah migration-lite foundation.
- Tambah integration test auth happy path.
- Tambah role access test matrix.
- Rapikan FE API client return shape.

Output:

- Project lebih aman untuk dijalankan dan diubah.
- Risiko config/secret/schema berkurang.

### Phase 2: Backend Contract dan ETL Reliability

Status: Completed

Estimasi: 2-4 minggu

- Standarkan response schema analytics.
- Tambah ETL quality report.
- Tambah ETL status summary endpoint.
- Tambah per-source row count dan timing.
- Pastikan manual ETL tetap single-source per run.
- Tambah mock-based tests untuk source ETL penting.

Output:

- Dashboard update data lebih reliable.
- Perubahan backend lebih aman untuk Streamlit.

### Phase 3: Analytics Performance

Estimasi: 2-4 minggu

- Audit query analytics lambat.
- Tambah index yang relevan.
- Tambah bounded date range dan pagination.
- Terapkan retention policy per tipe data.
- Tambah summary table untuk metric harian yang berat.
- Tambah query timing log.
- Tambah admin/status page untuk DB status, backup status, ETL history, dan slow-query report.

Output:

- Dashboard tetap cepat saat data bertambah.
- Beban SQLite lebih terkendali.

### Phase 4: Maintainability Cleanup

Estimasi: ongoing

- Refactor domain demi domain ke pola endpoint-service-repository-schema.
- Kurangi business logic di Streamlit page.
- Tambah ADR untuk keputusan teknis penting.
- Tambah developer checklist dan CI bila repo sudah siap.

Output:

- Fitur baru lebih mudah ditambah.
- Codebase lebih konsisten dan mudah direview.

## 10. Prioritas Backlog

| Priority | Item | Impact | Effort |
| --- | --- | --- | --- |
| P0 | Rotate secret dan sterilkan `.env.example` | Security | Low |
| P0 | Pertahankan hard fail `WORKERS=1` untuk SQLite | Data safety | Done/Low |
| P0 | Prevent ETL overlap dan expose status run | Data integrity | Medium |
| P1 | Migration-lite untuk SQLite | Maintainability | Medium |
| P1 | API response schema untuk endpoint utama | FE-BE contract | Medium |
| P1 | Integration test auth flow | Regression safety | Medium |
| P1 | SQLite backup/restore/integrity documentation | Operations | Low |
| P1 | Index audit analytics query | Performance | Medium |
| P1 | Admin/status page untuk DB, backup, ETL history, dan slow-query report | Operations | Medium |
| P2 | Materialized summary table | Performance | High |
| P2 | Role permission registry | Maintainability | Medium |
| P2 | Retention cleanup per tipe data | Data lifecycle | Medium |
| P2 | Query timing and slow endpoint logging | Observability | Low |
| P3 | ADR docs | Developer experience | Low |
| P3 | Optional pre-commit | Developer experience | Low |

## 11. Risks dan Mitigasi

| Risk | Dampak | Mitigasi |
| --- | --- | --- |
| SQLite write lock saat ETL dan request bersamaan | Request lambat/gagal | Serial ETL, busy timeout, WAL, bounded writes |
| DB membesar karena log dan data historis | Query lambat, backup berat | Retention, index, summary table, vacuum terjadwal |
| External API berubah | ETL gagal | Contract tests, typed transform, error detail per source |
| Role FE dan BE tidak sinkron | Unauthorized/overexposed page | Permission registry dan test matrix |
| Streamlit page terlalu banyak business logic | Sulit maintain | Pindahkan calculation ke backend service |
| Secret bocor dari `.env` | Security incident | Rotate secret, `.env.example`, secret checklist |

## 12. Product Decisions

Keputusan berikut menggantikan open questions awal dan menjadi constraint untuk roadmap berikutnya:

- Role `sales` belum perlu akses aktif dalam waktu dekat. Role tetap dikenal sistem, tetapi tidak mendapat navigasi aktif sampai kebutuhan produk berubah.
- Default retention untuk analytics aggregate adalah 24 bulan.
- Revenue, deposit, dan acquisition disimpan 36 bulan karena lebih penting untuk long-term business analysis.
- Data detail level media/post disimpan 12 bulan untuk menjaga ukuran SQLite tetap sehat.
- Request log dibatasi 30-90 hari.
- ETL run log disimpan 180 hari.
- Auth audit log disimpan 12 bulan.
- Export CSV/XLSX dari dashboard belum diperlukan.
- Manual ETL multi-source belum diperlukan. Manual ETL tetap single-source per run.
- Query analytics dianggap lambat jika berjalan lebih dari 2 detik.
- Query di atas 5 detik harus masuk slow-query report/admin status.
- Query di atas 10 detik dianggap critical dan perlu investigasi.
- Timeout endpoint analytics ditargetkan maksimal 30-60 detik, tergantung jenis laporan.
- Admin/status page diperlukan untuk DB status, backup status, ETL history, dan slow-query report.

## 13. Definition of Done

Satu improvement dianggap selesai bila:

- Perubahan mengikuti stack yang disepakati: Streamlit, FastAPI, SQLite.
- Ada test yang relevan atau alasan eksplisit bila tidak perlu.
- Tidak memperkenalkan multi-worker SQLite behavior.
- Tidak menambah secret ke repo.
- README atau docs diperbarui bila behavior/deployment berubah.
- Existing dashboard flow tetap berjalan.
- Error handling user-facing jelas dan tidak membocorkan detail sensitif.

## 14. Rekomendasi Implementasi Pertama

Urutan paling masuk akal untuk dikerjakan lebih dulu:

1. Buat `.env.example` dan secret rotation checklist.
2. Tambah SQLite maintenance script untuk integrity check, backup verification, dan optional vacuum.
3. Tambah migration-lite foundation.
4. Standarkan API client response di Streamlit.
5. Tambah integration test auth flow dan role access matrix.
6. Tambah ETL status summary endpoint dan row count tracking.
7. Audit index query analytics utama.
8. Tambah admin/status page untuk DB status, backup status, ETL history, dan slow-query report.
9. Terapkan retention cleanup sesuai policy data lifecycle.

Urutan ini memberi fondasi kuat tanpa rewrite besar dan tetap menghormati constraint utama project.
