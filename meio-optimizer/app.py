import streamlit as st
import pandas as pd

# -----------------------------
# DEBUG (VERY IMPORTANT)
# -----------------------------
st.write("APP IS RUNNING")

# -----------------------------
# LOAD DATA
# -----------------------------
try:
    df = pd.read_csv("meio-optimizer/sample_data.csv")
    st.success("Data loaded successfully")
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# -----------------------------
# TITLE
# -----------------------------
st.title("📦 MEIO Inventory Optimizer Dashboard")

# -----------------------------
# METRICS
# -----------------------------
st.subheader("📊 Key Metrics")

col1, col2, col3 = st.columns(3)

col1.metric("Avg Demand", round(df["avg_demand"].mean(), 2))
col2.metric("Avg Safety Stock", round(df["safety_stock"].mean(), 2))
col3.metric("Avg Lead Time", round(df["lead_time"].mean(), 2))

# -----------------------------
# TABLE
# -----------------------------
st.subheader("📋 Full Data")
st.dataframe(df)

# -----------------------------
# CHART
# -----------------------------
st.subheader("📈 Safety Stock by SKU")

chart = df.groupby("sku_id")["safety_stock"].mean()
st.bar_chart(chart)