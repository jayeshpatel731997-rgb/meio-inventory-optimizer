import os
import math
import streamlit as st
import pandas as pd

# ==============================
# PAGE CONFIG
# ==============================
st.set_page_config(
    page_title="MEIO Decision Intelligence Dashboard",
    layout="wide"
)

# ==============================
# HELPERS
# ==============================
def approx_z_score(service_level_pct: int) -> float:
    z_map = {
        85: 1.04,
        86: 1.08,
        87: 1.13,
        88: 1.17,
        89: 1.23,
        90: 1.28,
        91: 1.34,
        92: 1.41,
        93: 1.48,
        94: 1.55,
        95: 1.65,
        96: 1.75,
        97: 1.88,
        98: 2.05,
        99: 2.33,
    }
    return z_map.get(service_level_pct, 1.65)


@st.cache_data
def load_data() -> pd.DataFrame:
    base_dir = os.path.dirname(__file__)
    file_path = os.path.join(base_dir, "sample_data.csv")
    return pd.read_csv(file_path)


# ==============================
# LOAD DATA
# ==============================
df = load_data()

# Clean numeric columns just in case
numeric_cols = [
    "avg_demand",
    "std_demand",
    "lead_time",
    "reorder_point",
    "safety_stock",
]
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

# ==============================
# SIDEBAR CONTROLS
# ==============================
st.sidebar.header("⚙ Optimization Controls")

service_level = st.sidebar.slider(
    "Target Service Level (%)",
    min_value=85,
    max_value=99,
    value=95,
    step=1,
)

holding_cost_per_unit = st.sidebar.number_input(
    "Holding Cost per Unit ($)",
    min_value=0.0,
    value=2.0,
    step=0.5,
)

stockout_cost_per_unit = st.sidebar.number_input(
    "Stockout Cost per Unit ($)",
    min_value=0.0,
    value=10.0,
    step=1.0,
)

st.sidebar.header("🔎 Filters")

sku_options = ["All"] + sorted(df["sku_id"].astype(str).unique().tolist())
location_options = ["All"] + sorted(df["location_id"].astype(str).unique().tolist())

selected_sku = st.sidebar.selectbox("Select SKU", sku_options)
selected_location = st.sidebar.selectbox("Select Location", location_options)

filtered_df = df.copy()

if selected_sku != "All":
    filtered_df = filtered_df[filtered_df["sku_id"] == selected_sku]

if selected_location != "All":
    filtered_df = filtered_df[filtered_df["location_id"] == selected_location]

# ==============================
# MODEL RECALCULATION
# ==============================
z_score = approx_z_score(service_level)

filtered_df = filtered_df.copy()
filtered_df["calc_safety_stock"] = (
    z_score
    * filtered_df["std_demand"]
    * filtered_df["lead_time"].apply(lambda x: math.sqrt(x) if x > 0 else 0)
).round(2)

filtered_df["calc_reorder_point"] = (
    filtered_df["avg_demand"] * filtered_df["lead_time"]
    + filtered_df["calc_safety_stock"]
).round(2)

filtered_df["holding_cost_total"] = (
    filtered_df["calc_safety_stock"] * holding_cost_per_unit
).round(2)

filtered_df["risk_cost"] = (
    filtered_df["std_demand"] * stockout_cost_per_unit
).round(2)

filtered_df["total_cost"] = (
    filtered_df["holding_cost_total"] + filtered_df["risk_cost"]
).round(2)

filtered_df["recommendation"] = filtered_df.apply(
    lambda row: (
        "Increase buffer"
        if row["calc_safety_stock"] > row["safety_stock"]
        else "Reduce buffer"
        if row["calc_safety_stock"] < row["safety_stock"]
        else "Keep current policy"
    ),
    axis=1,
)

high_risk_df = filtered_df[
    filtered_df["calc_safety_stock"] > filtered_df["avg_demand"]
].copy()

top_cost_df = filtered_df.sort_values("total_cost", ascending=False).head(5)

# ==============================
# HEADER
# ==============================
st.title("📦 MEIO Decision Intelligence Dashboard")
st.caption(
    "Interactive inventory optimization with service-level simulation, cost trade-offs, and SKU risk insights."
)

# ==============================
# KPI CARDS
# ==============================
st.subheader("📊 Executive Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Filtered SKUs", int(filtered_df["sku_id"].nunique()))
col2.metric("Avg Simulated Safety Stock", round(filtered_df["calc_safety_stock"].mean(), 2))
col3.metric("Avg Simulated Reorder Point", round(filtered_df["calc_reorder_point"].mean(), 2))
col4.metric("Total Estimated Cost ($)", round(filtered_df["total_cost"].sum(), 2))

st.divider()

# ==============================
# MODEL SETTINGS SUMMARY
# ==============================
st.subheader("🧠 Scenario Settings")

s1, s2, s3 = st.columns(3)
s1.info(f"Service Level: **{service_level}%**")
s2.info(f"Approx Z-Score: **{z_score}**")
s3.info(f"Holding / Stockout Cost: **${holding_cost_per_unit} / ${stockout_cost_per_unit}**")

st.divider()

# ==============================
# CHARTS
# ==============================
c1, c2 = st.columns(2)

with c1:
    st.subheader("📈 Simulated Safety Stock by SKU")
    chart_ss = (
        filtered_df.groupby("sku_id", as_index=True)["calc_safety_stock"]
        .mean()
        .sort_values(ascending=False)
    )
    st.bar_chart(chart_ss)

with c2:
    st.subheader("💰 Total Cost by SKU")
    chart_cost = (
        filtered_df.groupby("sku_id", as_index=True)["total_cost"]
        .sum()
        .sort_values(ascending=False)
    )
    st.bar_chart(chart_cost)

st.divider()

# ==============================
# INSIGHTS
# ==============================
i1, i2 = st.columns(2)

with i1:
    st.subheader("⚠ Top 5 Costliest SKUs")
    st.dataframe(
        top_cost_df[
            [
                "sku_id",
                "location_id",
                "calc_safety_stock",
                "holding_cost_total",
                "risk_cost",
                "total_cost",
                "recommendation",
            ]
        ],
        use_container_width=True,
    )

with i2:
    st.subheader("🚨 High Risk SKUs")
    st.write(f"{len(high_risk_df)} SKU-location combinations have simulated safety stock above average demand.")
    st.dataframe(
        high_risk_df[
            [
                "sku_id",
                "location_id",
                "avg_demand",
                "calc_safety_stock",
                "calc_reorder_point",
                "recommendation",
            ]
        ],
        use_container_width=True,
    )

st.divider()

# ==============================
# POLICY COMPARISON
# ==============================
st.subheader("🔄 Current vs Simulated Policy")

comparison_cols = [
    "sku_id",
    "location_id",
    "avg_demand",
    "std_demand",
    "lead_time",
    "safety_stock",
    "calc_safety_stock",
    "reorder_point",
    "calc_reorder_point",
    "holding_cost_total",
    "risk_cost",
    "total_cost",
    "recommendation",
]

st.dataframe(filtered_df[comparison_cols], use_container_width=True)

st.divider()

# ==============================
# DOWNLOAD
# ==============================
st.subheader("⬇ Export Scenario Output")

csv_data = filtered_df.to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download simulated inventory policy as CSV",
    data=csv_data,
    file_name="meio_simulated_policy.csv",
    mime="text/csv",
)

# ==============================
# FOOTER
# ==============================
st.markdown("---")
st.caption(
    "Built by Jayesh | PostgreSQL + Python + Streamlit | MEIO + Service Level + Cost Trade-off Simulation"
)
