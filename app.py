import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="MEIO Dashboard", layout="wide")

st.markdown("## 📦 Supply Chain Decision Engine")
st.title("📊 MEIO Inventory Optimization Dashboard")

# =========================
# DB CONNECTION
# =========================
engine = create_engine("postgresql://postgres:Jayesh%4073@localhost:5432/meio_optimizer_db")

# =========================
# LOAD DATA
# =========================
df = pd.read_sql("SELECT * FROM opt_inventory_policy", engine)

# =========================
# SIDEBAR
# =========================
st.sidebar.header("⚙️ Controls")

service_level = st.sidebar.slider("Service Level", 0.80, 0.99, 0.95)

sku = st.sidebar.selectbox("Select SKU", df["sku_id"].unique())
location = st.sidebar.selectbox("Select Location", df["location_id"].unique())

filtered = df[(df["sku_id"] == sku) & (df["location_id"] == location)]

# =========================
# KPIs
# =========================
col1, col2, col3 = st.columns(3)

col1.metric("📦 Avg Demand", round(filtered["avg_demand"].values[0], 2))
col2.metric("🔁 Reorder Point", round(filtered["reorder_point"].values[0], 2))
col3.metric("🛡️ Safety Stock", round(filtered["safety_stock"].values[0], 2))

st.divider()

# =========================
# TOP RISK SKUs
# =========================
st.subheader("🔥 Top Risk SKUs (Highest Safety Stock)")

top_risk = df.sort_values("safety_stock", ascending=False).head(5)
st.dataframe(top_risk)

# =========================
# FULL DATA
# =========================
st.subheader("📊 Full Inventory Data")
st.dataframe(df)

# =========================
# VISUALIZATION
# =========================
st.subheader("📈 Demand vs Safety Stock")

chart_df = filtered[["avg_demand", "safety_stock", "reorder_point"]].T
chart_df.columns = ["Value"]

st.bar_chart(chart_df)

# =========================
# INSIGHTS
# =========================
st.subheader("🧠 Insights")

if filtered["safety_stock"].values[0] > filtered["avg_demand"].values[0]:
    st.warning("⚠️ High uncertainty — maintain higher buffer stock")
else:
    st.success("✅ Stable demand — lean inventory possible")

# =========================
# FOOTER
# =========================
st.markdown("---")
st.markdown("Built using Python + PostgreSQL + Monte Carlo Simulation 🚀")