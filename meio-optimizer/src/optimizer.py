import math
from statistics import NormalDist

import pandas as pd


MIN_COEFFICIENT_OF_VARIATION = 0.15


def _safe_float(value, default=0.0) -> float:
    if value is None or pd.isna(value):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _service_level_to_z(service_level: float) -> float:
    clipped = min(max(_safe_float(service_level, 0.95), 0.50), 0.999)
    return NormalDist().inv_cdf(clipped)


def calculate_safety_stock(
    avg_demand,
    std_demand,
    lead_time,
    target_service_level,
    min_cv: float = MIN_COEFFICIENT_OF_VARIATION,
) -> float:
    avg_demand_value = max(_safe_float(avg_demand), 0.0)
    std_demand_value = max(_safe_float(std_demand), 0.0)
    lead_time_value = max(_safe_float(lead_time, 1.0), 1.0)

    # When observed variability is zero or missing, use a minimum coefficient-of-variation
    # fallback so the planner does not get unrealistic zero safety stock recommendations.
    effective_std_demand = std_demand_value or max(avg_demand_value * min_cv, 1.0)
    z_score = _service_level_to_z(target_service_level)
    return round(z_score * effective_std_demand * math.sqrt(lead_time_value), 2)


def calculate_reorder_point(avg_demand, lead_time, safety_stock) -> float:
    return round(max(_safe_float(avg_demand), 0.0) * max(_safe_float(lead_time, 0.0), 0.0) + max(_safe_float(safety_stock), 0.0), 2)


def calculate_days_of_supply(on_hand_qty, avg_demand):
    avg_demand_value = _safe_float(avg_demand)
    if avg_demand_value <= 0:
        return None
    return round(max(_safe_float(on_hand_qty), 0.0) / avg_demand_value, 2)


def calculate_inventory_position(on_hand_qty, on_order_qty, backorder_qty) -> float:
    return round(
        max(_safe_float(on_hand_qty), 0.0)
        + max(_safe_float(on_order_qty), 0.0)
        - max(_safe_float(backorder_qty), 0.0),
        2,
    )


def optimize_inventory_policy(
    data: pd.DataFrame,
    target_service_level: float = 0.95,
    holding_cost_per_unit: float = 2.0,
    stockout_cost_per_unit: float = 10.0,
) -> pd.DataFrame:
    policy_df = data.copy()

    numeric_columns = [
        "avg_demand",
        "std_demand",
        "lead_time",
        "safety_stock",
        "reorder_point",
        "on_hand_qty",
        "on_order_qty",
        "backorder_qty",
        "unit_cost",
        "current_safety_stock_policy",
        "current_reorder_point_policy",
        "recommended_safety_stock",
        "recommended_reorder_point",
    ]
    for column in numeric_columns:
        if column not in policy_df.columns:
            policy_df[column] = 0.0
        policy_df[column] = pd.to_numeric(policy_df[column], errors="coerce")

    policy_df["inventory_position"] = policy_df.apply(
        lambda row: calculate_inventory_position(
            row.get("on_hand_qty"),
            row.get("on_order_qty"),
            row.get("backorder_qty"),
        ),
        axis=1,
    )
    policy_df["optimized_safety_stock"] = policy_df.apply(
        lambda row: calculate_safety_stock(
            row.get("avg_demand"),
            row.get("std_demand"),
            row.get("lead_time"),
            target_service_level,
        ),
        axis=1,
    )
    policy_df["optimized_reorder_point"] = policy_df.apply(
        lambda row: calculate_reorder_point(
            row.get("avg_demand"),
            row.get("lead_time"),
            row.get("optimized_safety_stock"),
        ),
        axis=1,
    )
    policy_df["days_of_supply"] = policy_df.apply(
        lambda row: calculate_days_of_supply(row.get("on_hand_qty"), row.get("avg_demand")),
        axis=1,
    )
    policy_df["inventory_investment"] = (
        policy_df["optimized_safety_stock"].fillna(0.0) * policy_df["unit_cost"].fillna(0.0)
    ).round(2)
    policy_df["holding_cost_total"] = (
        policy_df["optimized_safety_stock"].fillna(0.0) * _safe_float(holding_cost_per_unit, 2.0)
    ).round(2)
    policy_df["stockout_exposure_estimate"] = (
        policy_df["std_demand"].fillna(0.0).clip(lower=1.0)
        * _safe_float(stockout_cost_per_unit, 10.0)
        * (1 - target_service_level)
    ).round(2)
    policy_df["total_estimated_cost"] = (
        policy_df["holding_cost_total"] + policy_df["stockout_exposure_estimate"]
    ).round(2)
    policy_df["policy_gap_units"] = (
        policy_df["optimized_reorder_point"] - policy_df["inventory_position"]
    ).round(2)
    policy_df["target_service_level"] = round(target_service_level, 4)
    policy_df["optimized_z_score"] = round(_service_level_to_z(target_service_level), 4)

    def _action(row):
        if row["inventory_position"] < row["optimized_reorder_point"]:
            return "Increase buffer"
        if row["inventory_position"] > row["optimized_reorder_point"] * 1.5:
            return "Reduce buffer"
        return "Keep current policy"

    policy_df["recommendation"] = policy_df.apply(_action, axis=1)
    return policy_df
