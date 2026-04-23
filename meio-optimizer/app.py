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

# Sidebar filters
st.sidebar.header("Filters")

selected_sku = st.sidebar.selectbox(
    "Select SKU",
    options=["All"] + list(df["sku_id"].unique())
)

selected_location = st.sidebar.selectbox(
    "Select Location",
    options=["All"] + list(df["location_id"].unique())
)

# Apply filters
filtered_df = df.copy()

if selected_sku != "All":
    filtered_df = filtered_df[filtered_df["sku_id"] == selected_sku]

if selected_location != "All":
    filtered_df = filtered_df[filtered_df["location_id"] == selected_location]

# -----------------------------
# TABLE
# -----------------------------
st.subheader("📋 Full Data")
st.dataframe(filtered_df)

# -----------------------------
# CHART
# -----------------------------
st.subheader("📈 Safety Stock by SKU")

chart = filtered_df.groupby("sku_id")["safety_stock"].mean()
st.bar_chart(chart)

# -----------------------------
# BUSINESS INSIGHT
# -----------------------------
high_risk = filtered_df[filtered_df["safety_stock"] > filtered_df["avg_demand"]]

st.subheader("⚠️ High Risk SKUs")

st.write(f"{len(high_risk)} SKUs have high safety stock requirements")

st.dataframe(high_risk)