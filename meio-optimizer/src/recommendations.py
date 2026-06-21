import pandas as pd


PRIORITY_ORDER = {"Critical": 1, "High": 2, "Medium": 3, "Low": 4}


def _confidence(row) -> str:
    observation_days = pd.to_numeric(pd.Series([row.get("observation_days")]), errors="coerce").iloc[0]
    if pd.notna(observation_days) and observation_days >= 30:
        return "High"
    if pd.notna(observation_days) and observation_days >= 10:
        return "Medium"
    return "Low"


def generate_recommendations(policy_df: pd.DataFrame) -> pd.DataFrame:
    if policy_df is None or policy_df.empty:
        return pd.DataFrame(
            columns=[
                "priority",
                "sku",
                "location",
                "issue",
                "recommendation",
                "estimated_impact",
                "confidence",
            ]
        )

    recommendations = []
    for _, row in policy_df.iterrows():
        inventory_position = float(row.get("inventory_position", 0) or 0)
        reorder_point = float(row.get("optimized_reorder_point", row.get("reorder_point", 0)) or 0)
        days_of_supply = row.get("days_of_supply")
        unit_cost = float(row.get("unit_cost", 0) or 0)
        safety_stock_gap = float(row.get("optimized_safety_stock", 0) or 0) - float(
            row.get("safety_stock", row.get("current_safety_stock_policy", 0)) or 0
        )

        issue = None
        recommendation = None
        priority = "Low"

        if inventory_position < reorder_point and (days_of_supply is None or days_of_supply < 7):
            issue = "Understock risk"
            recommendation = "Expedite replenishment and raise safety stock."
            priority = "Critical"
        elif inventory_position < reorder_point:
            issue = "Inventory below reorder point"
            recommendation = "Trigger replenishment and monitor service level."
            priority = "High"
        elif days_of_supply is not None and days_of_supply > 60:
            issue = "Excess inventory"
            recommendation = "Reduce reorder point or rebalance stock."
            priority = "Medium"
        elif float(row.get("std_demand", 0) or 0) > float(row.get("avg_demand", 0) or 0):
            issue = "High demand volatility"
            recommendation = "Review forecast quality and add buffer coverage."
            priority = "Medium"

        if issue is None:
            continue

        recommendations.append(
            {
                "priority": priority,
                "sku": row.get("sku_id"),
                "location": row.get("location_id"),
                "issue": issue,
                "recommendation": recommendation,
                "estimated_impact": round(abs(safety_stock_gap) * unit_cost, 2),
                "confidence": _confidence(row),
            }
        )

    recommendation_df = pd.DataFrame(recommendations)
    if recommendation_df.empty:
        return pd.DataFrame(
            columns=[
                "priority",
                "sku",
                "location",
                "issue",
                "recommendation",
                "estimated_impact",
                "confidence",
            ]
        )

    recommendation_df["_priority_rank"] = recommendation_df["priority"].map(PRIORITY_ORDER)
    recommendation_df = recommendation_df.sort_values(
        by=["_priority_rank", "estimated_impact"], ascending=[True, False]
    ).drop(columns="_priority_rank")
    return recommendation_df.reset_index(drop=True)
