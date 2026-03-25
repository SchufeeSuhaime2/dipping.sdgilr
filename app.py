import os
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st

DB_FILE = "dipping.db"
TANK_MASTER_FILE = "data/tank_master.csv"
TANK_TABLE_FOLDER = "data/tank_tables"
DENSITY_TABLE_FILE = "data/density_table.csv"


def clean_product_code(value):
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8").strip()
        except Exception:
            return "".join(chr(b) for b in value if 32 <= b <= 126)

    if hasattr(value, "tolist") and not isinstance(value, str):
        value = value.tolist()

    if isinstance(value, (list, tuple)):
        try:
            return "".join(chr(int(x)) for x in value)
        except Exception:
            return "".join(str(x) for x in value)

    text = str(value).strip()

    if "," in text and all(part.strip().isdigit() for part in text.split(",")):
        try:
            return "".join(chr(int(part.strip())) for part in text.split(","))
        except Exception:
            return text

    return text


def clean_excel_text(value):
    if pd.isna(value):
        return value

    text = str(value)
    cleaned = "".join(
        ch for ch in text
        if ch in ("\t", "\n", "\r") or ord(ch) >= 32
    )
    return cleaned


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dipping_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_date TEXT,
            tank_no TEXT,
            product_code TEXT,
            product_desc TEXT,
            temp_c REAL,
            density REAL,
            dipping_level_mm REAL,
            dipping_mark_mm REAL,
            empty_space_mm REAL,
            flowmeter REAL,
            volume_litres REAL,
            tonnage_mt REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("PRAGMA table_info(dipping_records)")
    columns = [row[1] for row in cursor.fetchall()]

    if "density" not in columns:
        cursor.execute("ALTER TABLE dipping_records ADD COLUMN density REAL")

    if "tonnage_mt" not in columns:
        cursor.execute("ALTER TABLE dipping_records ADD COLUMN tonnage_mt REAL")

    conn.commit()
    conn.close()


def save_record(record):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    product_code = clean_product_code(record["product_code"])

    cursor.execute("""
        INSERT INTO dipping_records (
            record_date, tank_no, product_code, product_desc,
            temp_c, density, dipping_level_mm, dipping_mark_mm,
            empty_space_mm, flowmeter, volume_litres, tonnage_mt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(record["record_date"]),
        str(record["tank_no"]),
        product_code,
        str(record["product_desc"]),
        float(record["temp_c"]),
        float(record["density"]),
        float(record["dipping_level_mm"]),
        float(record["dipping_mark_mm"]),
        float(record["empty_space_mm"]),
        float(record["flowmeter"]),
        float(record["volume_litres"]),
        float(record["tonnage_mt"]),
    ))

    conn.commit()
    conn.close()


def delete_records(record_ids):
    if not record_ids:
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.executemany(
        "DELETE FROM dipping_records WHERE id = ?",
        [(int(record_id),) for record_id in record_ids]
    )

    conn.commit()
    conn.close()


def load_records(selected_date):
    conn = sqlite3.connect(DB_FILE)
    query = """
        SELECT id, record_date, tank_no, product_code, product_desc,
               temp_c, density, dipping_level_mm, dipping_mark_mm,
               empty_space_mm, flowmeter, volume_litres, tonnage_mt, created_at
        FROM dipping_records
        WHERE record_date = ?
        ORDER BY created_at ASC
    """
    df = pd.read_sql_query(query, conn, params=(selected_date,))
    conn.close()

    if "product_code" in df.columns:
        df["product_code"] = df["product_code"].apply(clean_product_code)

    return df


@st.cache_data
def load_tank_master():
    df = pd.read_csv(
        TANK_MASTER_FILE,
        dtype={"tank_no": str, "product_code": str, "product_desc": str}
    )
    df["tank_no"] = df["tank_no"].astype(str).str.strip()
    df["product_code"] = df["product_code"].apply(clean_product_code)
    df["product_desc"] = df["product_desc"].astype(str).str.strip()
    return df


@st.cache_data
def load_tank_table(tank_no):
    file_path = os.path.join(TANK_TABLE_FOLDER, f"{tank_no}.csv")
    if not os.path.exists(file_path):
        return None

    df = pd.read_csv(file_path)
    df["ullage_mm"] = pd.to_numeric(df["ullage_mm"], errors="coerce")
    df["volume_litres"] = pd.to_numeric(df["volume_litres"], errors="coerce")
    df = df.dropna(subset=["ullage_mm", "volume_litres"]).copy()
    df = df.drop_duplicates(subset=["ullage_mm"])
    df = df.sort_values("ullage_mm").reset_index(drop=True)
    return df


@st.cache_data
def load_density_table():
    if not os.path.exists(DENSITY_TABLE_FILE):
        return None

    df = pd.read_csv(DENSITY_TABLE_FILE)
    df["product_code"] = df["product_code"].astype(str).apply(clean_product_code)
    df["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")
    df["density"] = pd.to_numeric(df["density"], errors="coerce")
    df = df.dropna(subset=["product_code", "temp_c", "density"]).copy()
    df = df.drop_duplicates(subset=["product_code", "temp_c"])
    df = df.sort_values(["product_code", "temp_c"]).reset_index(drop=True)
    return df


def find_volume_from_ullage(tank_no, empty_space_mm):
    df = load_tank_table(tank_no)
    if df is None or df.empty:
        return None

    exact_match = df[df["ullage_mm"] == empty_space_mm]
    if not exact_match.empty:
        return float(exact_match.iloc[0]["volume_litres"])

    min_ullage = df["ullage_mm"].min()
    max_ullage = df["ullage_mm"].max()

    if empty_space_mm < min_ullage or empty_space_mm > max_ullage:
        return None

    lower_rows = df[df["ullage_mm"] < empty_space_mm]
    upper_rows = df[df["ullage_mm"] > empty_space_mm]

    if lower_rows.empty or upper_rows.empty:
        return None

    lower_row = lower_rows.iloc[-1]
    upper_row = upper_rows.iloc[0]

    x1 = float(lower_row["ullage_mm"])
    y1 = float(lower_row["volume_litres"])
    x2 = float(upper_row["ullage_mm"])
    y2 = float(upper_row["volume_litres"])

    if x2 == x1:
        return y1

    interpolated_volume = y1 + ((empty_space_mm - x1) / (x2 - x1)) * (y2 - y1)
    return float(interpolated_volume)


def find_density(product_code, temp_c):
    df = load_density_table()
    if df is None or df.empty:
        return None

    product_code = clean_product_code(product_code)
    df_product = df[df["product_code"] == product_code].copy()
    if df_product.empty:
        return None

    exact_match = df_product[df_product["temp_c"] == temp_c]
    if not exact_match.empty:
        return float(exact_match.iloc[0]["density"])

    min_temp = df_product["temp_c"].min()
    max_temp = df_product["temp_c"].max()

    if temp_c < min_temp or temp_c > max_temp:
        return None

    lower_rows = df_product[df_product["temp_c"] < temp_c]
    upper_rows = df_product[df_product["temp_c"] > temp_c]

    if lower_rows.empty or upper_rows.empty:
        return None

    lower_row = lower_rows.iloc[-1]
    upper_row = upper_rows.iloc[0]

    x1 = float(lower_row["temp_c"])
    y1 = float(lower_row["density"])
    x2 = float(upper_row["temp_c"])
    y2 = float(upper_row["density"])

    if x2 == x1:
        return y1

    interpolated_density = y1 + ((temp_c - x1) / (x2 - x1)) * (y2 - y1)
    return float(interpolated_density)


def calculate_tonnage(volume_litres, density):
    if volume_litres is None or density is None:
        return None
    return float((volume_litres * density) / 1000)


def prepare_display_table(records_df):
    display_df = records_df.copy()

    display_df.insert(0, "No", range(1, len(display_df) + 1))
    display_df["Delete"] = False

    display_df = display_df.rename(columns={
        "tank_no": "Tank No",
        "product_code": "Product Code",
        "product_desc": "Product Desc",
        "temp_c": "Temp (°C)",
        "density": "Density",
        "dipping_level_mm": "Dipping Level (mm)",
        "dipping_mark_mm": "Dipping Mark (mm)",
        "empty_space_mm": "Empty Space (mm)",
        "flowmeter": "Flowmeter",
        "volume_litres": "Volume (L)",
        "tonnage_mt": "Tonnage (MT)",
    })

    columns_order = [
        "id",
        "No",
        "Tank No",
        "Product Code",
        "Product Desc",
        "Temp (°C)",
        "Density",
        "Dipping Level (mm)",
        "Dipping Mark (mm)",
        "Empty Space (mm)",
        "Flowmeter",
        "Volume (L)",
        "Tonnage (MT)",
        "Delete",
    ]

    display_df = display_df[columns_order]

    if "Density" in display_df.columns:
        display_df["Density"] = display_df["Density"].map(lambda x: round(x, 4) if pd.notna(x) else None)
    if "Temp (°C)" in display_df.columns:
        display_df["Temp (°C)"] = display_df["Temp (°C)"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Dipping Level (mm)" in display_df.columns:
        display_df["Dipping Level (mm)"] = display_df["Dipping Level (mm)"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Dipping Mark (mm)" in display_df.columns:
        display_df["Dipping Mark (mm)"] = display_df["Dipping Mark (mm)"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Empty Space (mm)" in display_df.columns:
        display_df["Empty Space (mm)"] = display_df["Empty Space (mm)"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Flowmeter" in display_df.columns:
        display_df["Flowmeter"] = display_df["Flowmeter"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Volume (L)" in display_df.columns:
        display_df["Volume (L)"] = display_df["Volume (L)"].map(lambda x: round(x, 1) if pd.notna(x) else None)
    if "Tonnage (MT)" in display_df.columns:
        display_df["Tonnage (MT)"] = display_df["Tonnage (MT)"].map(lambda x: round(x, 3) if pd.notna(x) else None)

    return display_df


def main():
    st.set_page_config(page_title="Daily Dipping App", layout="wide")

    st.markdown("""
    <style>
    /* Make all st.data_editor text centered */
    div[data-testid="stDataEditor"] [role="columnheader"] {
        text-align: center !important;
        justify-content: center !important;
        display: flex !important;
        align-items: center !important;
    }

    div[data-testid="stDataEditor"] [role="columnheader"] * {
        text-align: center !important;
        justify-content: center !important;
    }

    div[data-testid="stDataEditor"] [role="gridcell"] {
        text-align: center !important;
        justify-content: center !important;
        display: flex !important;
        align-items: center !important;
    }

    div[data-testid="stDataEditor"] [role="gridcell"] * {
        text-align: center !important;
        justify-content: center !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }

    /* Fallback for older Streamlit table structure */
    div[data-testid="stDataEditor"] table td,
    div[data-testid="stDataEditor"] table th {
        text-align: center !important;
        vertical-align: middle !important;
    }

    div[data-testid="stDataEditor"] table td div,
    div[data-testid="stDataEditor"] table th div {
        justify-content: center !important;
        text-align: center !important;
    }

    /* center checkbox */
    div[data-testid="stDataEditor"] input[type="checkbox"] {
        display: block !important;
        margin-left: auto !important;
        margin-right: auto !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.title("Daily Dipping Tank Stock")
    st.caption("MVP version")

    init_db()
    tank_master = load_tank_master()

    tab1, tab2 = st.tabs(["Daily Entry", "Daily Records"])

    with tab1:
        st.subheader("Enter Dipping Data")

        col1, col2, col3 = st.columns(3)

        with col1:
            record_date = st.date_input("Date", value=date.today())

        with col2:
            tank_no = st.selectbox("Tank No", tank_master["tank_no"].tolist())

        tank_info = tank_master[tank_master["tank_no"] == tank_no].iloc[0]
        product_code = clean_product_code(tank_info["product_code"])
        product_desc = str(tank_info["product_desc"]).strip()

        with col3:
            flowmeter = st.number_input("Flowmeter", min_value=0.0, value=0.0, step=1.0)

        col4, col5, col6 = st.columns(3)

        with col4:
            temp_c = st.number_input("Temperature (°C)", min_value=0.0, value=60.0, step=0.1)

        with col5:
            dipping_level_mm = st.number_input("Dipping Level (mm)", min_value=0.0, value=0.0, step=1.0)

        with col6:
            dipping_mark_mm = st.number_input("Dipping Mark (mm)", min_value=0.0, value=0.0, step=1.0)

        empty_space_mm = dipping_level_mm - dipping_mark_mm

        volume_litres = None
        density = None
        tonnage_mt = None

        if empty_space_mm >= 0:
            volume_litres = find_volume_from_ullage(tank_no, empty_space_mm)

        density = find_density(product_code, temp_c)

        if volume_litres is not None and density is not None:
            tonnage_mt = calculate_tonnage(volume_litres, density)

        st.markdown("### Auto Result")
        r1, r2, r3 = st.columns(3)

        with r1:
            st.info(f"Product Code: {product_code}")

        with r2:
            st.info(f"Product Desc: {product_desc}")

        with r3:
            st.info(f"Empty Space (mm): {empty_space_mm:.1f}")

        if volume_litres is not None:
            st.success(f"Estimated Volume: {volume_litres:,.0f} L")
            st.caption(f"Exact calculated volume: {volume_litres:,.2f} L")
        else:
            st.warning("Volume not found. Please check tank table or ullage range.")

        if density is not None:
            st.success(f"Density: {density:.4f}")
        else:
            st.warning("Density not found. Please check density table or temperature range.")

        if tonnage_mt is not None:
            st.success(f"Tonnage: {tonnage_mt:,.3f} MT")

        if dipping_mark_mm > dipping_level_mm:
            st.error("Dipping mark cannot be greater than dipping level.")

        if st.button("Save Record", type="primary"):
            if dipping_mark_mm > dipping_level_mm:
                st.error("Cannot save. Please check dipping values.")
            elif volume_litres is None:
                st.error("Cannot save. Volume could not be determined.")
            elif density is None:
                st.error("Cannot save. Density could not be determined.")
            elif tonnage_mt is None:
                st.error("Cannot save. Tonnage could not be determined.")
            else:
                record = {
                    "record_date": str(record_date),
                    "tank_no": tank_no,
                    "product_code": product_code,
                    "product_desc": product_desc,
                    "temp_c": temp_c,
                    "density": round(density, 4),
                    "dipping_level_mm": dipping_level_mm,
                    "dipping_mark_mm": dipping_mark_mm,
                    "empty_space_mm": empty_space_mm,
                    "flowmeter": flowmeter,
                    "volume_litres": round(volume_litres, 2),
                    "tonnage_mt": round(tonnage_mt, 3),
                }
                save_record(record)
                st.success("Record saved successfully.")
                st.rerun()

    with tab2:
        st.subheader("Daily Records")
        selected_date = st.date_input("Select Date to View", value=date.today(), key="view_date")
        records_df = load_records(str(selected_date))

        if records_df.empty:
            st.info("No records found for this date.")
        else:
            display_df = prepare_display_table(records_df)

            edited_df = st.data_editor(
                display_df,
                use_container_width=True,
                hide_index=True,
                disabled=[
                    "No",
                    "Tank No",
                    "Product Code",
                    "Product Desc",
                    "Temp (°C)",
                    "Density",
                    "Dipping Level (mm)",
                    "Dipping Mark (mm)",
                    "Empty Space (mm)",
                    "Flowmeter",
                    "Volume (L)",
                    "Tonnage (MT)",
                ],
                column_config={
                    "id": None,
                    "Delete": st.column_config.CheckboxColumn(
                        "Delete",
                        help="Tick row to delete",
                        default=False,
                    ),
                },
                key="daily_records_editor"
            )

            rows_to_delete = edited_df[edited_df["Delete"] == True]

            if not rows_to_delete.empty:
                st.warning(f"{len(rows_to_delete)} row(s) selected for delete.")

            if st.button("Delete Selected Row(s)", type="secondary"):
                if rows_to_delete.empty:
                    st.error("Please tick at least one row to delete.")
                else:
                    delete_ids = rows_to_delete["id"].tolist()
                    delete_records(delete_ids)
                    st.success(f"{len(delete_ids)} row(s) deleted successfully.")
                    st.rerun()

            output_file = f"dipping_records_{selected_date}.xlsx"

            export_df = records_df.copy()
            export_df = export_df.drop(columns=["id"], errors="ignore")

            for col in export_df.columns:
                if export_df[col].dtype == object:
                    export_df[col] = export_df[col].apply(clean_excel_text)

            export_df.to_excel(output_file, index=False)

            with open(output_file, "rb") as f:
                st.download_button(
                    label="Download Excel",
                    data=f,
                    file_name=output_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )


if __name__ == "__main__":
    main()