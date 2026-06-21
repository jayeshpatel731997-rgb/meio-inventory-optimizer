import logging

import pandas as pd

from src.db import has_database_config, read_table_or_query


LOGGER = logging.getLogger(__name__)


DEMAND_COLUMNS = [
    "sku_id",
    "location_id",
    "customer_segment",
    "category",
    "lead_time",
    "z_score",
    "target_fill_rate",
    "observation_days",
    "avg_demand",
    "std_demand",
    "max_demand_per_day",
    "min_demand_per_day",
    "reorder_point",
    "safety_stock",
]

NETWORK_COLUMNS = [
    "from_location",
    "from_location_name",
    "from_type",
    "to_location",
    "to_location_name",
    "to_type",
    "transport_mode",
    "sku_id",
    "category",
    "num_shipments",
    "total_units_shipped",
    "total_freight_cost",
    "avg_freight_per_shipment",
    "freight_cost_per_unit",
    "avg_actual_lead_time_days",
    "std_actual_lead_time_days",
    "on_time_pct",
    "contracted_cost_per_unit",
    "freight_variance_per_unit",
]

INVENTORY_COLUMNS = [
    "location_id",
    "location_name",
    "location_type",
    "region",
    "echelon",
    "sku_id",
    "description",
    "category",
    "unit_cost",
    "lead_time",
    "as_of_date",
    "on_hand_qty",
    "on_order_qty",
    "backorder_qty",
    "inventory_position",
    "current_safety_stock_policy",
    "current_reorder_point_policy",
    "recommended_safety_stock",
    "recommended_reorder_point",
    "avg_demand",
    "days_of_supply",
    "inventory_value_usd",
    "service_level_status",
    "stock_status",
    "safety_stock_gap_units",
    "safety_stock_gap_usd",
]

SERVICE_POLICY_COLUMNS = [
    "customer_segment",
    "target_fill_rate",
    "z_score",
    "penalty_stockout_per_unit",
    "priority_rank",
    "holding_cost_rate",
    "min_order_qty",
]

MEIO_DATASET_COLUMNS = [
    "sku_id",
    "location_id",
    "location_name",
    "location_type",
    "region",
    "echelon",
    "description",
    "category",
    "customer_segment",
    "lead_time",
    "avg_demand",
    "std_demand",
    "observation_days",
    "target_fill_rate",
    "z_score",
    "unit_cost",
    "safety_stock",
    "reorder_point",
    "current_safety_stock_policy",
    "current_reorder_point_policy",
    "recommended_safety_stock",
    "recommended_reorder_point",
    "on_hand_qty",
    "on_order_qty",
    "backorder_qty",
    "inventory_position",
    "days_of_supply",
    "inventory_value_usd",
    "service_level_status",
    "stock_status",
]

MART_TABLES = [
    "mart_demand_stats",
    "mart_inventory_position",
    "mart_cost_to_serve",
    "mart_network_flow",
    "mart_data_quality_report",
]


def _empty_frame(columns, warning: str) -> pd.DataFrame:
    frame = pd.DataFrame(columns=columns)
    frame.attrs["warning"] = warning
    LOGGER.warning(warning)
    return frame


def _safe_query(query: str, columns, dataset_name: str) -> pd.DataFrame:
    if not has_database_config():
        return _empty_frame(columns, f"{dataset_name} unavailable: DATABASE_URL is not set.")

    try:
        frame = read_table_or_query(query)
    except Exception as exc:
        return _empty_frame(columns, f"{dataset_name} unavailable: {exc}")

    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA

    if frame.empty:
        frame.attrs["warning"] = f"{dataset_name} query returned no rows."
        LOGGER.warning(frame.attrs["warning"])

    return frame[columns]


def load_demand_stats() -> pd.DataFrame:
    return _safe_query(
        """
        SELECT
            sku_id,
            location_id,
            customer_segment,
            category,
            lead_time_days AS lead_time,
            z_score,
            target_fill_rate,
            observation_days,
            avg_demand_per_day AS avg_demand,
            std_demand_per_day AS std_demand,
            max_demand_per_day,
            min_demand_per_day,
            reorder_point_units AS reorder_point,
            safety_stock_units AS safety_stock
        FROM public.mart_demand_stats
        """,
        DEMAND_COLUMNS,
        "Demand mart",
    )


def load_network_flow() -> pd.DataFrame:
    return _safe_query(
        """
        SELECT
            from_location,
            from_location_name,
            from_type,
            to_location,
            to_location_name,
            to_type,
            transport_mode,
            sku_id,
            category,
            num_shipments,
            total_units_shipped,
            total_freight_cost,
            avg_freight_per_shipment,
            freight_cost_per_unit,
            avg_actual_lead_time_days,
            std_actual_lead_time_days,
            on_time_pct,
            contracted_cost_per_unit,
            freight_variance_per_unit
        FROM public.mart_network_flow
        """,
        NETWORK_COLUMNS,
        "Network flow mart",
    )


def load_inventory_policy() -> pd.DataFrame:
    return _safe_query(
        """
        SELECT
            location_id,
            location_name,
            location_type,
            region,
            echelon,
            sku_id,
            description,
            category,
            unit_cost,
            lead_time_days AS lead_time,
            as_of_date,
            on_hand_qty,
            on_order_qty,
            backorder_qty,
            inventory_position,
            current_safety_stock_policy,
            current_reorder_point_policy,
            recommended_safety_stock,
            recommended_reorder_point,
            avg_demand_per_day AS avg_demand,
            days_of_supply,
            inventory_value_usd,
            service_level_status,
            stock_status,
            safety_stock_gap_units,
            safety_stock_gap_usd
        FROM public.mart_inventory_position
        """,
        INVENTORY_COLUMNS,
        "Inventory position mart",
    )


def load_service_policy() -> pd.DataFrame:
    return _safe_query(
        """
        SELECT
            customer_segment,
            target_fill_rate,
            z_score,
            penalty_stockout_per_unit,
            priority_rank,
            holding_cost_rate,
            min_order_qty
        FROM public.dim_service_policy
        """,
        SERVICE_POLICY_COLUMNS,
        "Service policy table",
    )


def load_mart_row_counts() -> pd.DataFrame:
    columns = ["table_name", "exists", "row_count", "error"]
    if not has_database_config():
        return _empty_frame(
            columns,
            "Mart row counts unavailable: DATABASE_URL is not set.",
        )

    rows = []
    for table_name in MART_TABLES:
        try:
            exists_df = read_table_or_query(
                f"SELECT to_regclass('public.{table_name}') IS NOT NULL AS exists"
            )
            exists = bool(exists_df["exists"].iloc[0])
            row_count = None
            if exists:
                count_df = read_table_or_query(f"SELECT COUNT(*)::bigint AS row_count FROM public.{table_name}")
                row_count = int(count_df["row_count"].iloc[0])
            rows.append({"table_name": table_name, "exists": exists, "row_count": row_count, "error": ""})
        except Exception as exc:
            LOGGER.warning("Mart row count failed for %s: %s", table_name, exc)
            rows.append(
                {
                    "table_name": table_name,
                    "exists": False,
                    "row_count": None,
                    "error": str(exc),
                }
            )

    return pd.DataFrame(rows, columns=columns)


def load_meio_dataset() -> pd.DataFrame:
    demand_df = load_demand_stats()
    inventory_df = load_inventory_policy()

    demand_warning = demand_df.attrs.get("warning")
    inventory_warning = inventory_df.attrs.get("warning")

    if demand_df.empty and inventory_df.empty:
        warning_parts = [part for part in [demand_warning, inventory_warning] if part]
        return _empty_frame(
            MEIO_DATASET_COLUMNS,
            " | ".join(warning_parts) if warning_parts else "MEIO dataset unavailable.",
        )

    merged_df = inventory_df.merge(
        demand_df,
        on=["sku_id", "location_id"],
        how="outer",
        suffixes=("_inventory", "_demand"),
    )

    merged_df["location_name"] = merged_df.get("location_name")
    merged_df["location_type"] = merged_df.get("location_type")
    merged_df["region"] = merged_df.get("region")
    merged_df["echelon"] = merged_df.get("echelon")
    merged_df["description"] = merged_df.get("description", pd.Series(dtype="object")).fillna(
        merged_df.get("sku_id")
    )
    merged_df["category"] = merged_df.get("category_inventory", merged_df.get("category_demand"))
    merged_df["category"] = merged_df["category"].fillna(
        merged_df.get("category_demand", pd.Series(dtype="object"))
    )
    merged_df["customer_segment"] = merged_df.get("customer_segment", pd.Series(dtype="object")).fillna(
        "STANDARD"
    )
    merged_df["lead_time"] = merged_df.get("lead_time_inventory").combine_first(
        merged_df.get("lead_time_demand")
    )
    merged_df["avg_demand"] = merged_df.get("avg_demand_inventory").combine_first(
        merged_df.get("avg_demand_demand")
    )
    merged_df["std_demand"] = merged_df.get("std_demand", pd.Series(dtype="float64"))
    merged_df["observation_days"] = merged_df.get("observation_days")
    merged_df["target_fill_rate"] = merged_df.get("target_fill_rate").fillna(0.95)
    merged_df["z_score"] = merged_df.get("z_score").fillna(1.65)
    merged_df["unit_cost"] = merged_df.get("unit_cost").fillna(0.0)
    merged_df["safety_stock"] = merged_df.get("current_safety_stock_policy").combine_first(
        merged_df.get("recommended_safety_stock")
    )
    merged_df["safety_stock"] = merged_df["safety_stock"].combine_first(merged_df.get("safety_stock"))
    merged_df["reorder_point"] = merged_df.get("current_reorder_point_policy").combine_first(
        merged_df.get("recommended_reorder_point")
    )
    merged_df["reorder_point"] = merged_df["reorder_point"].combine_first(merged_df.get("reorder_point"))

    for column in MEIO_DATASET_COLUMNS:
        if column not in merged_df.columns:
            merged_df[column] = pd.NA

    result = merged_df[MEIO_DATASET_COLUMNS].copy()
    if demand_warning or inventory_warning:
        result.attrs["warning"] = " | ".join(
            [part for part in [demand_warning, inventory_warning] if part]
        )
    return result
