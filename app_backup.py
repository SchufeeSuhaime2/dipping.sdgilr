import os
import sqlite3
from datetime import date

import pandas as pd
import streamlit as st

DB_FILE = "dipping.db"
TANK_MASTER_FILE = "data/tank_master.csv"
TANK_TABLE_FOLDER = "data/tank_tables"


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
            dipping_level_mm REAL,
            dipping_mark_mm REAL,
            empty_space_mm REAL,
            flowmeter REAL,
            volume_litres REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def save_record(record):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dipping_records (
            record_date, tank_no, product_code, product_desc,
            temp_c, dipping_level_mm, dipping_mark_mm,
            empty_space_mm, flowmeter, volume_litres
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record["record_date"],
        record["tank_no"],
        record["product_code"],
        record["product_desc"],
        record["temp_c"],
        record["dipping_level_mm"],
        record["dipping_mark_mm"],
        record["empty_space_mm"],
        record["flowmeter"],
        record["volume_litres"],
    ))

    conn.commit()
    conn.close()


def load_records(selected_date):
    conn = sqlite3.connect(DB_FILE)
    query = """
        SELECT record_date, tank_no, product_code, product_desc,
               temp_c, dipping_level_mm, dipping_mark_mm,
               empty_space_mm, flowmeter, volume_litres, created_at
        FROM dipping_records
        WHERE record_date = ?
        ORDER BY tank_no
    """
    df = pd.read_sql_query(query, conn, params=(selected_date,))
    conn.close()
    return df


@st.cache_data
def load_tank_master():
    return pd.read_csv(TANK_MASTER_FILE)


@st.cache_data
def load_tank_table(tank_no):
    file_path = os.path.join(TANK_TABLE_FOLDER, f"{tank_no}.csv")
    if not os.path.exists(file_path):
        return None

    df = pd.read_csv(file_path)
    df = df.sort_values("ullage_mm", ascending=False).reset_index(drop=True)
    return df


def find_volume_from_ullage(tank_no, empty_space_mm):
    df = load_tank_table(tank_no)
    if df is None or df.empty:
        return None

    exact_match = df[df["ullage_mm"] == empty_space_mm]
    if not exact_match.empty:
        return float(exact_match.iloc[0]["volume_litres"])

    df["difference"] = (df["ullage_mm"] - empty_space_mm).abs()
    nearest_row = df.loc[df["difference"].idxmin()]
    return float(nearest_row["volume_litres"])


def main():
    st.set_page_config(page_title="Daily Dipping App", layout="wide")
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
        product_code = tank_info["product_code"]
        product_desc = tank_info["product_desc"]

        with col3:
            flowmeter = st.number_input("Flowmeter", min_value=0.0, value=0.0, step=1.0)

        col4, col5, col6 = st.columns(3)

        with col4:
            temp_c = st.number_input("Temperature (°C)", min_value=0.0, value=50.0, step=0.1)

        with col5:
            dipping_level_mm = st.number_input("Dipping Level (mm)", min_value=0.0, value=0.0, step=1.0)

        with col6:
            dipping_mark_mm = st.number_input("Dipping Mark (mm)", min_value=0.0, value=0.0, step=1.0)

        empty_space_mm = dipping_level_mm - dipping_mark_mm

        st.markdown("### Auto Result")
        r1, r2, r3 = st.columns(3)

        with r1:
            st.info(f"Product Code: {product_code}")

        with r2:
            st.info(f"Product Desc: {product_desc}")

        with r3:
            st.info(f"Empty Space (mm): {empty_space_mm:.1f}")

        volume_litres = None
        if empty_space_mm >= 0:
            volume_litres = find_volume_from_ullage(tank_no, empty_space_mm)

        if volume_litres is not None:
            st.success(f"Estimated Volume: {volume_litres:,.0f} L")
        else:
            st.warning("No tank table found or no matching volume data.")

        if dipping_mark_mm > dipping_level_mm:
            st.error("Dipping mark cannot be greater than dipping level.")

        if st.button("Save Record", type="primary"):
            if dipping_mark_mm > dipping_level_mm:
                st.error("Cannot save. Please check dipping values.")
            elif volume_litres is None:
                st.error("Cannot save. Volume could not be determined.")
            else:
                record = {
                    "record_date": str(record_date),
                    "tank_no": tank_no,
                    "product_code": product_code,
                    "product_desc": product_desc,
                    "temp_c": temp_c,
                    "dipping_level_mm": dipping_level_mm,
                    "dipping_mark_mm": dipping_mark_mm,
                    "empty_space_mm": empty_space_mm,
                    "flowmeter": flowmeter,
                    "volume_litres": volume_litres,
                }
                save_record(record)
                st.success("Record saved successfully.")

    with tab2:
        st.subheader("Daily Records")
        selected_date = st.date_input("Select Date to View", value=date.today(), key="view_date")
        records_df = load_records(str(selected_date))

        if records_df.empty:
            st.info("No records found for this date.")
        else:
            st.dataframe(records_df, use_container_width=True)

            output_file = f"dipping_records_{selected_date}.xlsx"
            records_df.to_excel(output_file, index=False)

            with open(output_file, "rb") as f:
                st.download_button(
                    label="Download Excel",
                    data=f,
                    file_name=output_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )


if __name__ == "__main__":
    main()