from pathlib import Path

import pandas as pd
import streamlit as st

from src.data_access import load_mart_row_counts, load_meio_dataset, load_network_flow, load_service_policy
from src.data_quality import run_data_quality_checks
from src.db import get_database_info, get_database_url, has_database_config, mask_database_url, test_connection
from src.optimizer import optimize_inventory_policy
from src.recommendations import generate_recommendations
from src.scenario_engine import run_service_level_scenarios


APP_DIR = Path(__file__).resolve().parent
SAMPLE_DATA_PATH = APP_DIR / "sample_data.csv"


st.set_page_config(page_title="MEIO Decision Intelligence Dashboard", layout="wide")


def _coalesce_warning(*frames) -> list[str]:
    warnings = []
    for frame in frames:
        if hasattr(frame, "attrs") and frame.attrs.get("warning"):
            warnings.append(frame.attrs["warning"])
    return warnings


def _masked_database_url() -> str:
    return mask_database_url(get_database_url())


def _normalize_demo_data(frame: pd.DataFrame) -> pd.DataFrame:
    demo_df = frame.copy()
    numeric_columns = [
        "avg_demand",
        "std_demand",
        "lead_time",
        "reorder_point",
        "safety_stock",
    ]
    for column in numeric_columns:
        demo_df[column] = pd.to_numeric(demo_df[column], errors="coerce").fillna(0.0)

    demo_df["location_name"] = demo_df["location_id"]
    demo_df["location_type"] = "RDC"
    demo_df["region"] = "Demo Region"
    demo_df["echelon"] = 2
    demo_df["description"] = demo_df["sku_id"]
    demo_df["category"] = "Demo"
    demo_df["customer_segment"] = "STANDARD"
    demo_df["target_fill_rate"] = 0.95
    demo_df["z_score"] = 1.65
    demo_df["observation_days"] = 30
    demo_df["unit_cost"] = 10.0
    demo_df["current_reorder_point_policy"] = demo_df["reorder_point"]
    demo_df["current_safety_stock_policy"] = demo_df["safety_stock"]
    demo_df["recommended_reorder_point"] = demo_df["reorder_point"]
    demo_df["recommended_safety_stock"] = demo_df["safety_stock"]
    demo_df["on_hand_qty"] = (demo_df["reorder_point"] * 0.80).round(0)
    demo_df["on_order_qty"] = (demo_df["safety_stock"] * 0.25).round(0)
    demo_df["backorder_qty"] = 0.0
    demo_df["inventory_position"] = (
        demo_df["on_hand_qty"] + demo_df["on_order_qty"] - demo_df["backorder_qty"]
    )
    demo_df["days_of_supply"] = (
        demo_df["on_hand_qty"] / demo_df["avg_demand"].replace(0, pd.NA)
    ).round(2)
    demo_df["inventory_value_usd"] = (demo_df["on_hand_qty"] * demo_df["unit_cost"]).round(2)
    demo_df["service_level_status"] = "SAFE"
    demo_df["stock_status"] = "NORMAL"
    return demo_df


@st.cache_data
def load_csv_demo_data() -> pd.DataFrame:
    return _normalize_demo_data(pd.read_csv(SAMPLE_DATA_PATH))


@st.cache_data(ttl=300)
def load_postgres_bundle(database_url: str):
    # The argument is intentionally part of the Streamlit cache key so changing
    # DATABASE_URL cannot reuse stale CSV/failure-era database reads.
    meio_df = load_meio_dataset()
    network_df = load_network_flow()
    service_policy_df = load_service_policy()
    return meio_df, network_df, service_policy_df


@st.cache_data(ttl=300)
def load_database_health(database_url: str):
    # The argument is intentionally part of the Streamlit cache key.
    connected, message = test_connection()
    if connected:
        mart_counts_df = load_mart_row_counts()
    else:
        mart_counts_df = pd.DataFrame(columns=["table_name", "exists", "row_count", "error"])
    return connected, message, mart_counts_df


def load_application_data():
    notices = []
    network_df = pd.DataFrame()
    service_policy_df = pd.DataFrame()
    fallback_reason = None
    mart_counts_df = pd.DataFrame(columns=["table_name", "exists", "row_count", "error"])
    database_url = get_database_url()
    database_info = get_database_info()

    if database_url:
        if database_info.get("invalid_placeholder"):
            fallback_reason = "Invalid placeholder DATABASE_URL detected"
            notices.append(f"{fallback_reason}. Using CSV demo fallback.")
            return (
                load_csv_demo_data(),
                network_df,
                service_policy_df,
                "CSV demo fallback",
                notices,
                fallback_reason,
                False,
                fallback_reason,
                mart_counts_df,
            )

        connected, message = test_connection()
        if connected:
            mart_counts_df = load_mart_row_counts()
            missing_marts = []
            if not mart_counts_df.empty:
                missing_marts = mart_counts_df.loc[
                    mart_counts_df["exists"].astype(str).str.lower() != "true",
                    "table_name",
                ].tolist()

            required_marts = {"mart_demand_stats", "mart_inventory_position"}
            missing_required = sorted(required_marts.intersection(set(missing_marts)))
            if missing_required:
                fallback_reason = f"Required mart table(s) missing: {', '.join(missing_required)}"
                notices.append(f"{fallback_reason}. Using CSV demo fallback.")
                return (
                    load_csv_demo_data(),
                    network_df,
                    service_policy_df,
                    "CSV demo fallback",
                    notices,
                    fallback_reason,
                    connected,
                    message,
                    mart_counts_df,
                )
            if missing_marts:
                notices.append(f"Optional mart table(s) missing: {', '.join(sorted(missing_marts))}.")

            zero_row_marts = []
            if not mart_counts_df.empty:
                zero_row_marts = mart_counts_df.loc[
                    (mart_counts_df["exists"].astype(str).str.lower() == "true")
                    & (pd.to_numeric(mart_counts_df["row_count"], errors="coerce").fillna(-1) == 0),
                    "table_name",
                ].tolist()
            if zero_row_marts:
                notices.append(f"Mart table(s) exist but contain zero rows: {', '.join(zero_row_marts)}.")

            failed_count_checks = []
            if not mart_counts_df.empty and "error" in mart_counts_df.columns:
                failed_count_checks = mart_counts_df.loc[
                    mart_counts_df["error"].fillna("").astype(str) != "",
                    "table_name",
                ].tolist()
            if failed_count_checks:
                fallback_reason = f"Mart verification failed for table(s): {', '.join(failed_count_checks)}"
                notices.append(f"{fallback_reason}. Using CSV demo fallback.")
                return (
                    load_csv_demo_data(),
                    network_df,
                    service_policy_df,
                    "CSV demo fallback",
                    notices,
                    fallback_reason,
                    connected,
                    message,
                    mart_counts_df,
                )

            meio_df, network_df, service_policy_df = load_postgres_bundle(database_url)
            query_warnings = _coalesce_warning(meio_df, network_df, service_policy_df)
            notices.extend(query_warnings)
            query_failures = [
                warning for warning in query_warnings if "unavailable:" in warning.lower()
            ]
            if query_failures:
                fallback_reason = "PostgreSQL query failed: " + " | ".join(query_failures)
                notices.append(f"{fallback_reason}. Using CSV demo fallback.")
                return (
                    load_csv_demo_data(),
                    network_df,
                    service_policy_df,
                    "CSV demo fallback",
                    notices,
                    fallback_reason,
                    connected,
                    message,
                    mart_counts_df,
                )

            if meio_df.empty:
                notices.append(
                    "PostgreSQL connected, but the MEIO mart dataset returned zero rows. "
                    "Staying in PostgreSQL mode so database issues are visible."
                )
                return (
                    meio_df,
                    network_df,
                    service_policy_df,
                    "PostgreSQL marts connected",
                    notices,
                    None,
                    connected,
                    message,
                    mart_counts_df,
                )
            return (
                meio_df,
                network_df,
                service_policy_df,
                "PostgreSQL marts connected",
                notices,
                None,
                connected,
                message,
                mart_counts_df,
            )
        else:
            fallback_reason = f"Database connection failed: {message}"
            notices.append(f"{fallback_reason}. Using CSV demo fallback.")
    else:
        fallback_reason = "DATABASE_URL is not set."
        notices.append(f"{fallback_reason} Using CSV demo fallback.")

    return (
        load_csv_demo_data(),
        network_df,
        service_policy_df,
        "CSV demo fallback",
        notices,
        fallback_reason,
        False,
        fallback_reason or "Database not connected.",
        mart_counts_df,
    )


(
    data_df,
    network_df,
    service_policy_df,
    data_mode,
    notices,
    fallback_reason,
    db_connected,
    db_message,
    mart_counts_df,
) = load_application_data()
db_info = get_database_info()


st.title("MEIO Decision Intelligence Dashboard")
st.caption(
    "Inventory planning workspace with PostgreSQL mart support, service-level scenarios, "
    "planner actions, and demo-safe CSV fallback."
)


st.sidebar.header("Data Mode")
if data_mode == "PostgreSQL marts connected":
    st.sidebar.success(data_mode)
else:
    st.sidebar.info(data_mode)

st.sidebar.header("Database Debug")
st.sidebar.write(f"DATABASE_URL detected: {'yes' if db_info.get('detected') else 'no'}")
st.sidebar.write(f"Source: {db_info.get('source') or 'missing'}")
if db_info.get("env_file"):
    st.sidebar.caption(f".env file: {db_info['env_file']}")
st.sidebar.write(f"Masked DATABASE_URL: {db_info.get('masked_url') or 'not available'}")
st.sidebar.write(f"Database host: {db_info.get('host') or 'not available'}")
st.sidebar.write(f"Database name: {db_info.get('database') or 'not available'}")
st.sidebar.write(f"SQLAlchemy driver: {db_info.get('driver') or 'not available'}")
st.sidebar.write(f"Connection test: {'passed' if db_connected else 'failed'}")
if db_info.get("invalid_placeholder"):
    st.sidebar.error("Invalid placeholder DATABASE_URL detected")
if db_info.get("error"):
    st.sidebar.warning(f"URL parse error: {db_info['error']}")
if fallback_reason:
    st.sidebar.error(f"Fallback reason: {fallback_reason}")
if not mart_counts_df.empty:
    with st.sidebar.expander("Mart row counts", expanded=True):
        st.dataframe(mart_counts_df, hide_index=True, use_container_width=True)

for notice in notices:
    st.sidebar.warning(notice)


st.sidebar.header("Optimization Controls")
service_level_pct = st.sidebar.slider(
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

st.sidebar.header("Filters")
sku_options = ["All"] + sorted(data_df.get("sku_id", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())
location_options = ["All"] + sorted(data_df.get("location_id", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())

selected_sku = st.sidebar.selectbox("Select SKU", sku_options)
selected_location = st.sidebar.selectbox("Select Location", location_options)


filtered_source_df = data_df.copy()
if selected_sku != "All":
    filtered_source_df = filtered_source_df[filtered_source_df["sku_id"].astype(str) == selected_sku]
if selected_location != "All":
    filtered_source_df = filtered_source_df[filtered_source_df["location_id"].astype(str) == selected_location]

if filtered_source_df.empty:
    st.warning("No rows match the selected filters. Reset the filters to see inventory policy results.")
    filtered_source_df = data_df.copy()

optimized_df = optimize_inventory_policy(
    filtered_source_df,
    target_service_level=service_level_pct / 100,
    holding_cost_per_unit=holding_cost_per_unit,
    stockout_cost_per_unit=stockout_cost_per_unit,
)
scenario_df = run_service_level_scenarios(filtered_source_df)
recommendations_df = generate_recommendations(optimized_df)
data_quality_df = run_data_quality_checks(data_df)


top_cost_df = optimized_df.sort_values("total_estimated_cost", ascending=False).head(5)
high_risk_df = optimized_df[
    optimized_df["optimized_safety_stock"] > pd.to_numeric(optimized_df["avg_demand"], errors="coerce").fillna(0)
].copy()

if not network_df.empty:
    filtered_network_df = network_df.copy()
    if selected_sku != "All":
        filtered_network_df = filtered_network_df[
            filtered_network_df["sku_id"].astype(str) == selected_sku
        ]
else:
    filtered_network_df = network_df


tabs = st.tabs(
    [
        "Executive MEIO Dashboard",
        "Inventory Policy Table",
        "Service-Level Scenario Engine",
        "Network / Lane Risk Summary",
        "Planner Recommendations",
        "Data Health Checks",
        "Database Verification",
        "Technical Assumptions",
    ]
)


with tabs[0]:
    st.subheader("Executive Summary")
    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("Filtered SKUs", int(optimized_df["sku_id"].nunique()))
    metric_col2.metric(
        "Avg Optimized Safety Stock",
        round(optimized_df["optimized_safety_stock"].mean(), 2),
    )
    metric_col3.metric(
        "Avg Optimized Reorder Point",
        round(optimized_df["optimized_reorder_point"].mean(), 2),
    )
    metric_col4.metric(
        "Total Estimated Cost ($)",
        round(optimized_df["total_estimated_cost"].sum(), 2),
    )

    st.divider()
    st.subheader("Scenario Settings")
    settings_col1, settings_col2, settings_col3 = st.columns(3)
    settings_col1.info(f"Target service level: **{service_level_pct}%**")
    optimized_z_score = (
        pd.to_numeric(optimized_df.get("optimized_z_score"), errors="coerce").dropna().iloc[0]
        if not optimized_df.empty and "optimized_z_score" in optimized_df.columns
        else 0.0
    )
    settings_col2.info(
        f"Optimized z-score: **{optimized_z_score:.2f}**"
    )
    settings_col3.info(
        f"Holding / stockout cost: **${holding_cost_per_unit} / ${stockout_cost_per_unit}**"
    )

    st.divider()
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.subheader("Optimized Safety Stock by SKU")
        safety_stock_chart = (
            optimized_df.groupby("sku_id", as_index=True)["optimized_safety_stock"]
            .mean()
            .sort_values(ascending=False)
        )
        st.bar_chart(safety_stock_chart)
    with chart_col2:
        st.subheader("Estimated Cost by SKU")
        total_cost_chart = (
            optimized_df.groupby("sku_id", as_index=True)["total_estimated_cost"]
            .sum()
            .sort_values(ascending=False)
        )
        st.bar_chart(total_cost_chart)

    st.divider()
    insight_col1, insight_col2 = st.columns(2)
    with insight_col1:
        st.subheader("Top 5 Costliest SKU-Location Rows")
        st.dataframe(
            top_cost_df[
                [
                    "sku_id",
                    "location_id",
                    "optimized_safety_stock",
                    "holding_cost_total",
                    "stockout_exposure_estimate",
                    "total_estimated_cost",
                    "recommendation",
                ]
            ],
            use_container_width=True,
        )
    with insight_col2:
        st.subheader("High Risk Rows")
        st.write(
            f"{len(high_risk_df)} SKU-location rows have optimized safety stock above average demand."
        )
        st.dataframe(
            high_risk_df[
                [
                    "sku_id",
                    "location_id",
                    "avg_demand",
                    "optimized_safety_stock",
                    "optimized_reorder_point",
                    "recommendation",
                ]
            ],
            use_container_width=True,
        )


with tabs[1]:
    st.subheader("Inventory Policy Comparison")
    st.dataframe(
        optimized_df[
            [
                "sku_id",
                "location_id",
                "customer_segment",
                "avg_demand",
                "std_demand",
                "lead_time",
                "safety_stock",
                "optimized_safety_stock",
                "reorder_point",
                "optimized_reorder_point",
                "inventory_position",
                "days_of_supply",
                "holding_cost_total",
                "stockout_exposure_estimate",
                "total_estimated_cost",
                "recommendation",
            ]
        ],
        use_container_width=True,
    )

    st.subheader("Export Scenario Output")
    st.download_button(
        label="Download optimized inventory policy as CSV",
        data=optimized_df.to_csv(index=False).encode("utf-8"),
        file_name="meio_optimized_policy.csv",
        mime="text/csv",
    )


with tabs[2]:
    st.subheader("Service-Level Scenario Engine")
    st.dataframe(scenario_df, use_container_width=True)

    scenario_chart = scenario_df.set_index("service_level")[
        ["inventory_investment", "stockout_exposure_estimate"]
    ]
    st.bar_chart(scenario_chart)


with tabs[3]:
    st.subheader("Network / Lane Risk Summary")
    if filtered_network_df.empty:
        st.info("No network mart data is available yet. This section will populate when mart_network_flow exists.")
    else:
        lane_col1, lane_col2, lane_col3 = st.columns(3)
        lane_col1.metric("Active Lanes", int(len(filtered_network_df)))
        lane_col2.metric(
            "Avg On-Time %",
            round(pd.to_numeric(filtered_network_df["on_time_pct"], errors="coerce").mean(), 2),
        )
        lane_col3.metric(
            "Max Freight Variance / Unit",
            round(
                pd.to_numeric(filtered_network_df["freight_variance_per_unit"], errors="coerce")
                .abs()
                .max(),
                4,
            ),
        )

        st.dataframe(
            filtered_network_df.sort_values(
                by=["on_time_pct", "freight_variance_per_unit"],
                ascending=[True, False],
            ),
            use_container_width=True,
        )


with tabs[4]:
    st.subheader("Planner Recommendations")
    if recommendations_df.empty:
        st.info("No planner interventions are required for the current filter and scenario settings.")
    else:
        st.dataframe(recommendations_df, use_container_width=True)


with tabs[5]:
    st.subheader("Data Health Checks")
    st.dataframe(data_quality_df, use_container_width=True)

    if not service_policy_df.empty:
        st.subheader("Loaded Service Policy")
        st.dataframe(service_policy_df, use_container_width=True)


with tabs[6]:
    st.subheader("Database Verification")
    db_col1, db_col2, db_col3 = st.columns(3)
    db_col1.metric("DATABASE_URL configured", "Yes" if db_info.get("detected") else "No")
    db_col2.metric("Connection status", "Connected" if db_connected else "Not connected")
    db_col3.metric("Current data mode", data_mode)

    info_col1, info_col2, info_col3 = st.columns(3)
    info_col1.metric("Database host", db_info.get("host") or "Not available")
    info_col2.metric("Database name", db_info.get("database") or "Not available")
    info_col3.metric("Driver", db_info.get("driver") or "Not available")
    st.write(f"Source: **{db_info.get('source') or 'missing'}**")

    if has_database_config():
        st.code(_masked_database_url(), language="text")
    else:
        st.info("Set DATABASE_URL to enable PostgreSQL mart loading.")

    if db_connected:
        st.success(db_message)
    else:
        st.warning(db_message)

    if fallback_reason:
        st.error(f"Fallback reason: {fallback_reason}")
    else:
        st.success("Fallback reason: none. PostgreSQL marts are powering the dashboard.")

    st.subheader("Expected Mart Tables")
    if mart_counts_df.empty:
        st.info("Mart row counts are unavailable until PostgreSQL is connected.")
    else:
        st.dataframe(mart_counts_df, use_container_width=True)

    st.subheader("Fallback State")
    if data_mode == "PostgreSQL marts connected":
        st.success("The dashboard is reading PostgreSQL marts.")
    else:
        st.info("The dashboard is currently using sample_data.csv fallback.")


with tabs[7]:
    st.subheader("Technical Assumptions")
    st.markdown(
        """
        - The app prefers PostgreSQL mart tables when `DATABASE_URL` is available and reachable.
        - If the database is unavailable, the dashboard falls back to `sample_data.csv` so the app remains demo-safe.
        - Safety stock uses a service-level z-score and a minimum coefficient-of-variation fallback when observed variability is zero or missing.
        - Reorder point is calculated as average demand during lead time plus safety stock.
        - Inventory investment is estimated as optimized safety stock multiplied by unit cost.
        - Stockout exposure is a directional estimate for planning comparison, not a full probabilistic loss model.
        - Network and recommendation views depend on mart availability and will stay empty rather than crash when those marts are missing.
        """
    )


st.markdown("---")
st.caption(
    "Built by Jayesh | PostgreSQL + Python + Streamlit | MEIO planning, service-level simulation, and planner actions"
)
