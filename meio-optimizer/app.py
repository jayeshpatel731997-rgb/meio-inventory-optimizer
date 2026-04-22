import streamlit as st
import pandas as pd

st.set_page_config(page_title="MEIO Optimizer", layout="wide")

st.title("📦 MEIO Inventory Optimization Dashboard")

# Load CSV
df = pd.read_csv("sample_data.csv")

# Basic metrics
col1, col2, col3 = st.columns(3)

col1.metric("Total SKUs", df["sku_id"].nunique())
col2.metric("Avg Safety Stock", round(df["safety_stock"].mean(), 2))
col3.metric("Avg Lead Time", round(df["lead_time"].mean(), 2))

st.divider()

# Table
st.subheader("📊 Inventory Policy Table")
st.dataframe(df, use_container_width=True)

# Chart
st.subheader("📈 Safety Stock by SKU")
st.bar_chart(df.set_index("sku_id")["safety_stock"])