import streamlit as st
import pandas as pd
import os

# =========================
# DEBUG START
# =========================
st.write("🚀 App started")

st.write("📂 Files in current directory:")
st.write(os.listdir())

# =========================
# LOAD DATA SAFELY
# =========================
try:
    df = pd.read_csv("sample_data.csv")
    st.success("✅ CSV Loaded Successfully")
except Exception as e:
    st.error(f"❌ CSV Load Failed: {e}")
    st.stop()

# =========================
# DASHBOARD
# =========================
st.title("📦 MEIO Inventory Optimization Dashboard")

# KPIs
col1, col2, col3 = st.columns(3)

col1.metric("Total SKUs", df["sku_id"].nunique())
col2.metric("Avg Safety Stock", round(df["safety_stock"].mean(), 2))
col3.metric("Avg Lead Time", round(df["lead_time"].mean(), 2))

st.divider()

# TABLE
st.subheader("📊 Inventory Data")
st.dataframe(df, use_container_width=True)

# CHART
st.subheader("📈 Safety Stock by SKU")
st.bar_chart(df.set_index("sku_id")["safety_stock"])