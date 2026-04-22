import streamlit as st
import pandas as pd

# ==============================
# PAGE CONFIG
# ==============================
st.set_page_config(
    page_title="MEIO Inventory Optimizer",
    layout="wide"
)

# ==============================
# DEBUG (REMOVE LATER)
# ==============================
st.write("🚀 App started successfully")

# ==============================
# LOAD DATA
# ==============================
@st.cache_data
def load_data():
    return pd.read_csv("sample_data.csv")

df = load_data()

# ==============================
# TITLE
# ==============================
st.title("📦 MEIO Inventory Optimization Dashboard")
st.caption("Multi-Echelon Inventory Optimization using Monte Carlo Simulation")

# ==============================
# SIDEBAR FILTERS
# ==============================
st.sidebar.header("🔍 Filters")

sku_filter = st.sidebar.multiselect(
    "Select SKU",
    options=df["sku_id"].unique(),
    default=df["sku_id"].unique()
)

location_filter = st.sidebar.multiselect(
    "Select Location",
    options=df["location_id"].unique(),
    default=df["location_id"].unique()
)

filtered_df = df[
    (df["sku_id"].isin(sku_filter)) &
    (df["location_id"].isin(location_filter))
]

# ==============================
# KPI METRICS
# ==============================
st.subheader("📊 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total SKUs",
    filtered_df["sku_id"].nunique()
)

col2.metric(
    "Avg Safety Stock",
    round(filtered_df["safety_stock"].mean(), 2)
)

col3.metric(
    "Avg Lead Time",
    round(filtered_df["lead_time"].mean(), 2)
)

col4.metric(
    "Avg Reorder Point",
    round(filtered_df["reorder_point"].mean(), 2)
)

st.divider()

# ==============================
# CHARTS
# ==============================
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Safety Stock by SKU")
    chart_data = filtered_df.groupby("sku_id")["safety_stock"].mean()
    st.bar_chart(chart_data)

with col2:
    st.subheader("📊 Lead Time by SKU")
    chart_data = filtered_df.groupby("sku_id")["lead_time"].mean()
    st.bar_chart(chart_data)

st.divider()

# ==============================
# TOP RISK ITEMS
# ==============================
st.subheader("⚠️ High Safety Stock (Risk Items)")

top_risk = filtered_df.sort_values(
    by="safety_stock",
    ascending=False
).head(10)

st.dataframe(top_risk, use_container_width=True)

st.divider()

# ==============================
# FULL TABLE
# ==============================
st.subheader("📋 Full Inventory Policy Table")

st.dataframe(filtered_df, use_container_width=True)

# ==============================
# DOWNLOAD OPTION
# ==============================
csv = filtered_df.to_csv(index=False)

st.download_button(
    label="⬇️ Download Data",
    data=csv,
    file_name="meio_inventory_output.csv",
    mime="text/csv"
)

# ==============================
# FOOTER
# ==============================
st.markdown("---")
st.caption("Built by Jayesh | Supply Chain Analytics | MEIO Project")
