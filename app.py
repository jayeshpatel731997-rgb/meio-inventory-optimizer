import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import scipy.stats as stats

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="MEIO Optimizer",
    page_icon="📊",
    layout="wide"
)

st.title("📦 MEIO Inventory Optimizer")

# =========================
# DB CONNECTION
# =========================
@st.cache_resource
def get_engine():
    return create_engine("postgresql://postgres:Jayesh%4073@localhost:5432/meio_optimizer_db")

try:
    engine = get_engine()
    # Test connection
    with engine.connect() as conn:
        pass
    data_available = True
except Exception as e:
    st.error(f"Database connection failed: {e}")
    data_available = False

# =========================
# LOAD DATA
# =========================
if data_available:
    try:
        @st.cache_data
        def load_data():
            demand_df = pd.read_sql("SELECT * FROM mart_demand_stats", engine)
            flow_df = pd.read_sql("SELECT * FROM mart_network_flow", engine)
            return demand_df, flow_df
        
        demand_df, flow_df = load_data()
    except Exception as e:
        st.error(f"Data load failed: {e}")
        data_available = False

if data_available:
    # =========================
    # STEP 1: SERVICE LEVEL CONTROL
    # =========================
    st.sidebar.header("⚙️ Optimization Controls")
    
    service_level = st.sidebar.slider(
        "Service Level (%)",
        min_value=85,
        max_value=99,
        value=95,
        help="Higher service level = lower stockout risk but higher holding costs"
    )
    
    z_score = stats.norm.ppf(service_level / 100)
    
    st.sidebar.metric("Z-Score", f"{z_score:.3f}")
    
    # =========================
    # COST ASSUMPTIONS (STEP 4)
    # =========================
    st.sidebar.subheader("💰 Cost Assumptions")
    holding_cost = st.sidebar.number_input(
        "Holding Cost per Unit ($)",
        min_value=0.1,
        value=2.0,
        step=0.1,
        help="Annual cost to hold one unit of inventory"
    )
    
    stockout_cost = st.sidebar.number_input(
        "Stockout Cost per Unit ($)",
        min_value=0.1,
        value=10.0,
        step=0.1,
        help="Cost per unit of unmet demand"
    )
    
    # =========================
    # FILTERS
    # =========================
    st.sidebar.subheader("📋 Filters")
    
    skus = demand_df["sku_id"].unique()
    selected_skus = st.sidebar.multiselect(
        "Select SKUs",
        options=skus,
        default=list(skus[:10])
    )
    
    # =========================
    # PREP DATA
    # =========================
    agg_demand = demand_df[[
        "sku_id",
        "location_id",
        "avg_demand_per_day",
        "std_demand_per_day"
    ]].copy()
    
    agg_demand.rename(columns={
        "avg_demand_per_day": "avg_demand",
        "std_demand_per_day": "std_demand"
    }, inplace=True)
    
    # Lead time simulation
    lead_time = flow_df[["sku_id", "to_location"]].drop_duplicates().copy()
    lead_time["lead_time"] = np.random.randint(3, 10, size=len(lead_time))
    lead_time.rename(columns={"to_location": "location_id"}, inplace=True)
    
    # Merge
    df = agg_demand.merge(lead_time, on=["sku_id", "location_id"], how="inner")
    df["std_demand"] = df["std_demand"].fillna(0)
    
    # Filter by selected SKUs
    filtered_df = df[df["sku_id"].isin(selected_skus)].copy()
    
    # =========================
    # STEP 2: RECALCULATE SAFETY STOCK DYNAMICALLY
    # =========================
    filtered_df["calc_safety_stock"] = (
        z_score *
        filtered_df["std_demand"] *
        (filtered_df["lead_time"] ** 0.5)
    )
    
    filtered_df["calc_reorder_point"] = (
        filtered_df["avg_demand"] * filtered_df["lead_time"]
        + filtered_df["calc_safety_stock"]
    )
    
    # =========================
    # STEP 4: COST MODEL
    # =========================
    filtered_df["holding_cost_total"] = (
        filtered_df["calc_safety_stock"] * holding_cost
    )
    
    filtered_df["risk_cost"] = (
        filtered_df["std_demand"] * stockout_cost
    )
    
    filtered_df["total_cost"] = (
        filtered_df["holding_cost_total"] + filtered_df["risk_cost"]
    )
    
    # =========================
    # STEP 3: SHOW COMPARISON
    # =========================
    st.subheader("📊 Model vs Calculated Safety Stock")
    
    comparison_cols = [
        "sku_id",
        "location_id",
        "avg_demand",
        "std_demand",
        "lead_time",
        "calc_safety_stock",
        "calc_reorder_point"
    ]
    
    st.dataframe(
        filtered_df[comparison_cols].round(2),
        use_container_width=True,
        hide_index=True
    )
    
    # =========================
    # STEP 5: DECISION INSIGHTS
    # =========================
    st.subheader("💡 Insights & Recommendations")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Avg Safety Stock",
            f"{filtered_df['calc_safety_stock'].mean():.1f} units"
        )
    
    with col2:
        st.metric(
            "Total Holding Cost",
            f"${filtered_df['holding_cost_total'].sum():.2f}"
        )
    
    with col3:
        st.metric(
            "Total Risk Cost",
            f"${filtered_df['risk_cost'].sum():.2f}"
        )
    
    # Top 5 SKUs with highest holding cost
    st.write("**Top 5 SKUs with Highest Holding Cost:**")
    high_cost = filtered_df.sort_values(
        "holding_cost_total", ascending=False
    ).head(5)
    
    st.dataframe(
        high_cost[[
            "sku_id",
            "location_id",
            "calc_safety_stock",
            "holding_cost_total"
        ]].round(2),
        use_container_width=True,
        hide_index=True
    )
    
    # Top 5 SKUs with highest risk cost
    st.write("**Top 5 SKUs with Highest Risk Cost:**")
    high_risk = filtered_df.sort_values(
        "risk_cost", ascending=False
    ).head(5)
    
    st.dataframe(
        high_risk[[
            "sku_id",
            "location_id",
            "calc_safety_stock",
            "risk_cost"
        ]].round(2),
        use_container_width=True,
        hide_index=True
    )
    
    # =========================
    # STEP 6: VISUAL IMPACT
    # =========================
    st.subheader("📈 Cost vs Safety Stock Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Scatter: Holding Cost vs Safety Stock**")
        st.scatter_chart(
            filtered_df,
            x="calc_safety_stock",
            y="holding_cost_total",
            size="avg_demand",
            color="location_id"
        )
    
    with col2:
        st.write("**Scatter: Risk Cost vs Safety Stock**")
        st.scatter_chart(
            filtered_df,
            x="calc_safety_stock",
            y="risk_cost",
            size="avg_demand",
            color="location_id"
        )
    
    # Total cost distribution
    st.write("**Total Cost Distribution by SKU**")
    cost_by_sku = filtered_df.groupby("sku_id").agg({
        "holding_cost_total": "sum",
        "risk_cost": "sum",
        "calc_safety_stock": "mean"
    }).reset_index()
    
    st.bar_chart(
        cost_by_sku.set_index("sku_id")[["holding_cost_total", "risk_cost"]]
    )
    
    # =========================
    # EXPORT DATA
    # =========================
    st.subheader("📥 Export Results")
    
    csv = filtered_df[[
        "sku_id",
        "location_id",
        "avg_demand",
        "std_demand",
        "lead_time",
        "calc_safety_stock",
        "calc_reorder_point",
        "holding_cost_total",
        "risk_cost",
        "total_cost"
    ]].round(2).to_csv(index=False)
    
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="meio_inventory_policy.csv",
        mime="text/csv"
    )

else:
    st.error("Cannot load application without database connection.")
