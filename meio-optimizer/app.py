import streamlit as st
import pandas as pd

st.write("App is running 🚀")

st.set_page_config(page_title="MEIO Inventory Optimizer", layout="wide")

st.title("📦 MEIO Inventory Optimization Dashboard")

# Load data
try:
    df = pd.read_csv("sample_data.csv")
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# KPIs
col1, col2, col3 = st.columns(3)

col1.metric("Total SKUs", df["sku_id"].nunique())
col2.metric("Avg Safety Stock", round(df["safety_stock"].mean(), 2))
col3.metric("Avg Reorder Point", round(df["reorder_point"].mean(), 2))

st.divider()

# Table
st.subheader("📊 Inventory Optimization Results")
st.dataframe(df, use_container_width=True)

# Filter
st.subheader("🔍 Filter by SKU")
sku = st.selectbox("Select SKU", df["sku_id"].unique())

filtered_df = df[df["sku_id"] == sku]

st.write(filtered_df)

# Chart
st.subheader("📈 Safety Stock by Location")
st.bar_chart(df.groupby("location_id")["safety_stock"].mean())