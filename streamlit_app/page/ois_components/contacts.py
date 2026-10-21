"""Paginated OIS contact table and spreadsheet exports of the full filtered set."""
import csv
import hashlib
import io
from itertools import chain
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import streamlit as st

COLUMNS = ("email", "phone", "username", "tgl_regis", "utm_source", "utm_medium", "utm_campaign")
PAGE_SIZE = 25


def contact_frame(rows, selected_ids):
    records = [row for row in rows if row.get("tf_id") in selected_ids]
    df = pd.DataFrame(records, columns=["tf_id", *COLUMNS])
    return df.sort_values(["tgl_regis", "tf_id"], ascending=[False, True])[list(COLUMNS)].fillna("")


def csv_bytes(df):
    """Protect spreadsheet programs from interpreting source strings as formulas."""
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(COLUMNS)
    for row in df.itertuples(index=False, name=None):
        writer.writerow(["'" + str(value) if str(value).lstrip().startswith(("=", "+", "-", "@"))
                         else str(value) for value in row])
    return output.getvalue().encode("utf-8-sig")


def xlsx_bytes(df):
    """Write a text-only OOXML workbook, preserving phone prefixes and timestamps.

    All seven columns are source text. Inline strings require no optional Excel
    engine and cannot execute formulas; the small package uses the OOXML minimum.
    """
    output = io.BytesIO()
    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '''<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>''')
        archive.writestr("_rels/.rels", '''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>''')
        archive.writestr("xl/workbook.xml", f'''<workbook xmlns="{namespace}" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="OIS Contacts" sheetId="1" r:id="rId1"/></sheets></workbook>''')
        archive.writestr("xl/_rels/workbook.xml.rels", '''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>''')
        with archive.open("xl/worksheets/sheet1.xml", "w") as sheet:
            sheet.write(
                f'<worksheet xmlns="{namespace}"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" state="frozen"/></sheetView></sheetViews><cols><col min="1" max="7" width="24" customWidth="1"/></cols><sheetData>'.encode())
            for number, row in enumerate(chain((COLUMNS,), df.itertuples(index=False, name=None)), 1):
                cells = []
                for index, value in enumerate(row):
                    # XML 1.0 does not permit control characters except tab/newline/CR.
                    text = ''.join(c for c in str(value) if ord(c) >= 32 or c in '\t\n\r')
                    cells.append(
                        f'<c r="{chr(65 + index)}{number}" t="inlineStr"><is><t xml:space="preserve">{escape(text)}</t></is></c>')
                sheet.write(f'<row r="{number}">{"".join(cells)}</row>'.encode())
            sheet.write(f'</sheetData><autoFilter ref="A1:G{len(df) + 1}"/></worksheet>'.encode())
    return output.getvalue()


def render_contacts(rows, selected_ids, dates):
    df = contact_frame(rows, selected_ids)
    st.markdown("## Register Contacts")
    with st.container(border=True):
        if df.empty:
            st.info("No contact records match these filters.")
            return
        fingerprint = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()
        if st.session_state.get("ois_contacts_fingerprint") != fingerprint:
            st.session_state["ois_contacts_page"] = 1
            st.session_state["ois_contacts_fingerprint"] = fingerprint
        page_count = max(1, (len(df) + PAGE_SIZE - 1) // PAGE_SIZE)
        pagination, csv_column, excel_column = st.columns([2, 1, 1], gap="small", vertical_alignment="bottom")
        page = pagination.number_input("Page", min_value=1, max_value=page_count,
                                       step=1, key="ois_contacts_page")
        start = (page - 1) * PAGE_SIZE
        st.dataframe(df.iloc[start:start + PAGE_SIZE], hide_index=True, width="stretch")
        st.caption(f"{start + 1}–{min(start + PAGE_SIZE, len(df))} of {len(df):,} records · 25 per page")
        filename = f"ois_contacts_{dates[0]}_{dates[1]}"
        csv_column.download_button("Export CSV", csv_bytes(df), file_name=filename + ".csv",
                                   mime="text/csv", key="ois_contacts_csv", width="stretch")
        excel_column.download_button("Export XLSX", xlsx_bytes(df), file_name=filename + ".xlsx",
                                     mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                     key="ois_contacts_xlsx", width="stretch")
