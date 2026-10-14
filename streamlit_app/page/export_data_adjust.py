"""Admin Adjust table export with a date-filtered preview."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
import httpx
import streamlit as st
from streamlit_app.functions.runtime import resolve_backend_base_url, refresh_backend_tokens


async def request(path, params=None):
    host = resolve_backend_base_url(prefer_internal=True)
    async with httpx.AsyncClient(timeout=300) as client:
        headers = {"Authorization": f"Bearer {st.session_state.get('access_token', '')}"}
        response = await client.get(f"{host}/api/adjust-export/{path}", params=params, headers=headers)
        if response.status_code == 401:
            refreshed = await refresh_backend_tokens(host=host)
            if refreshed and refreshed.get("success"):
                st.session_state.access_token = refreshed["access_token"]
                headers["Authorization"] = f"Bearer {refreshed['access_token']}"
                response = await client.get(f"{host}/api/adjust-export/{path}", params=params, headers=headers)
        response.raise_for_status()
        return response


async def show_export_data_adjust_page(host):
    del host
    if st.session_state.get("role") not in {"admin", "superadmin"}:
        st.error("Halaman ini hanya untuk admin.")
        return
    st.title("Export Data Adjust")
    st.caption("Pilih tabel dan rentang tanggal untuk mengunduh seluruh kolom sebagai CSV. Tanggal menggunakan WIB.")
    try:
        tables = (await request("tables")).json()["tables"]
        by_name = {item["table"]: item for item in tables}
        selected = st.selectbox("Tabel Adjust", list(by_name), key="adjust_export_table")
        bounds = by_name[selected]
        lower = date.fromisoformat(bounds["data_start"]) if bounds["data_start"] else datetime.now(ZoneInfo("Asia/Jakarta")).date()
        upper = date.fromisoformat(bounds["data_end"]) if bounds["data_end"] else datetime.now(ZoneInfo("Asia/Jakarta")).date()
        dates = st.date_input("Rentang tanggal", value=(lower, upper), key=f"adjust_export_dates_{selected}", format="DD/MM/YYYY")
        st.caption("Data difilter berdasarkan tanggal aktivitas dalam WIB.")
        if len(dates) != 2:
            st.info("Pilih tanggal awal dan akhir.")
            return
        start, end = dates
        if start > end:
            st.error("Tanggal awal harus sebelum atau sama dengan tanggal akhir.")
            return
        params = {"table": selected, "start_date": start.isoformat(), "end_date": end.isoformat()}
        info = (await request("summary", params)).json()
        a, b, c = st.columns(3)
        a.metric("Jumlah baris", f"{info['row_count']:,}")
        b.metric("Tanggal data pertama", info["data_start"] or "—")
        c.metric("Tanggal data terakhir", info["data_end"] or "—")
        st.caption(f"Rentang export: {start:%d/%m/%Y} sampai {end:%d/%m/%Y} (inklusif).")
        signature = (st.session_state.get("access_token"), selected, start, end)
        prepared = st.session_state.get("adjust_export_prepared")
        if prepared and prepared["signature"] != signature:
            st.session_state.pop("adjust_export_prepared", None)
            prepared = None
        if not info["row_count"]:
            st.info("Tidak ada data pada rentang tanggal ini.")
            return
        if st.button("Siapkan CSV", type="primary"):
            with st.spinner("Menyiapkan export..."):
                response = await request("csv", params)
                prepared = dict(signature=signature, content=response.content,
                                count=int(response.headers["X-Export-Row-Count"]))
                st.session_state.adjust_export_prepared = prepared
        if prepared:
            st.success(f"CSV siap: {prepared['count']:,} baris untuk {start:%d/%m/%Y} sampai {end:%d/%m/%Y}.")
            st.download_button("Download CSV", data=prepared["content"], file_name=f"{selected}_{start}_{end}.csv", mime="text/csv")
    except httpx.HTTPStatusError as error:
        try: detail = error.response.json().get("detail")
        except ValueError: detail = None
        st.error(detail or "Export tidak dapat dimuat. Periksa akses akun dan coba lagi.")
    except httpx.RequestError:
        st.error("Backend tidak dapat dihubungi. Coba lagi.")
