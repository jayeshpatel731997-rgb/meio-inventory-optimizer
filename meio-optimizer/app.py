import streamlit as st
import pandas as pd
import os

# Get current file directory
BASE_DIR = os.path.dirname(__file__)

# Build correct path
file_path = os.path.join(BASE_DIR, "sample_data.csv")

df = pd.read_csv(file_path)

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