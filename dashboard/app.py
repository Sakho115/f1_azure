"""
Formula 1 Lakehouse - Enterprise Analytics Dashboard.
Built with Streamlit, Plotly, DuckDB, and Delta Lake.
Serves as the local open-source equivalent to Power BI and Databricks SQL Dashboards.
"""

import os
import sys
import glob
import pandas as pd
import duckdb
import streamlit as st

# Configure Streamlit page layout
st.set_page_config(
    page_title="Formula 1 Lakehouse Analytics",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add project root to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

from dashboard.components.standings_view import render_standings_view
from dashboard.components.telemetry_view import render_telemetry_view
from dashboard.components.circuit_view import render_circuit_view

GOLD_PATH = os.path.join(BASE_DIR, "data", "gold")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")

@st.cache_data(ttl=60)
def load_delta_table(table_path: str) -> pd.DataFrame:
    """
    Loads Delta table data using DuckDB or direct Parquet resolution for rapid sub-second query performance.
    """
    if not os.path.exists(table_path):
        return pd.DataFrame()
    
    # Locate active parquet data files (excluding delta log internal files)
    parquet_files = [
        f for f in glob.glob(os.path.join(table_path, "**", "*.parquet"), recursive=True)
        if "_delta_log" not in f
    ]
    
    if not parquet_files:
        return pd.DataFrame()

    con = duckdb.connect(database=":memory:")
    try:
        # Query via DuckDB
        files_str = "['" + "', '".join(parquet_files) + "']"
        query = f"SELECT * FROM read_parquet({files_str})"
        return con.execute(query).df()
    except Exception as e:
        # Fallback to pandas read_parquet
        dfs = [pd.read_parquet(p) for p in parquet_files]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    finally:
        con.close()

def main():
    # Sidebar
    st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg", width=120)
    st.sidebar.title("F1 Lakehouse Intelligence")
    st.sidebar.markdown("**Enterprise Medallion Architecture**")
    st.sidebar.markdown("---")

    st.sidebar.markdown("### 🏛️ Architecture Mapping")
    st.sidebar.info(
        """
        - **Storage:** Local HNS ↔ ADLS Gen2
        - **Compute:** PySpark ↔ Databricks
        - **Format:** Delta Lake ACID ↔ Delta
        - **Orchestration:** Python / Airflow ↔ ADF
        - **Catalog:** Local Catalog ↔ Unity Catalog
        - **BI:** Streamlit ↔ Power BI
        """
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("Data Source: Ergast Formula 1 Historical API")

    # Load Core Datasets
    driver_standings = load_delta_table(os.path.join(GOLD_PATH, "driver_standings"))
    constructor_standings = load_delta_table(os.path.join(GOLD_PATH, "constructor_standings"))
    race_results_gold = load_delta_table(os.path.join(GOLD_PATH, "race_results_gold"))
    pit_stop_analysis = load_delta_table(os.path.join(GOLD_PATH, "pit_stop_analysis"))
    qualifying_analysis = load_delta_table(os.path.join(GOLD_PATH, "qualifying_analysis"))
    circuits_silver = load_delta_table(os.path.join(SILVER_PATH, "circuits"))

    if driver_standings.empty or race_results_gold.empty:
        st.warning("⚠️ Lakehouse Gold Delta tables not detected or empty.")
        st.info("Run the pipeline first using: `python orchestration/pipeline.py --stage all`")
        return

    # Navigation Tabs
    tab_standings, tab_telemetry, tab_circuits, tab_arch = st.tabs([
        "🏆 Championship Standings",
        "⚡ Race Telemetry & Pit Stops",
        "🌍 World Circuits",
        "📐 Medallion Lakehouse Architecture"
    ])

    with tab_standings:
        render_standings_view(driver_standings, constructor_standings)

    with tab_telemetry:
        render_telemetry_view(race_results_gold, pit_stop_analysis, qualifying_analysis)

    with tab_circuits:
        render_circuit_view(circuits_silver)

    with tab_arch:
        st.header("📐 Medallion Architecture & Lineage")
        st.markdown(
            """
            ### Pipeline Data Flow:
            1. **Raw Tier (`data/raw/`):** Ingestion of multi-format Ergast source feeds (CSV, JSON, multi-part).
            2. **Bronze Tier (`data/bronze/`):** Schema-enforced Delta tables with audit provenance (`ingestion_date`, `input_file_name`).
            3. **Silver Tier (`data/silver/`):** Cleaned, normalized Delta tables with SCD-1 dimension upserts and incremental fact MERGE.
            4. **Gold Tier (`data/gold/`):** Curated business analytics marts, standings aggregates, and telemetry models.
            """
        )

        st.subheader("Data Mart Row Counts (Active Lakehouse State)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Bronze Circuits", len(circuits_silver))
        col2.metric("Silver Race Results", len(race_results_gold))
        col3.metric("Gold Driver Records", len(driver_standings))

if __name__ == "__main__":
    main()
