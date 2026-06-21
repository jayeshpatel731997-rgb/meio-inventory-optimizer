import pandas as pd

from src.optimizer import optimize_inventory_policy


def run_service_level_scenarios(data, service_levels=[0.90, 0.95, 0.97, 0.99]) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame(
            columns=[
                "service_level",
                "safety_stock",
                "reorder_point",
                "inventory_investment",
                "carrying_cost",
                "stockout_exposure_estimate",
                "change_vs_current",
            ]
        )

    current_safety_stock = pd.to_numeric(data.get("safety_stock"), errors="coerce").fillna(0.0)
    unit_cost = pd.to_numeric(data.get("unit_cost"), errors="coerce").fillna(0.0)
    current_investment = (current_safety_stock * unit_cost).sum()

    scenarios = []
    for level in service_levels:
        scenario_df = optimize_inventory_policy(data, target_service_level=level)
        inventory_investment = scenario_df["inventory_investment"].sum()
        carrying_cost = (
            scenario_df["inventory_investment"]
            * pd.to_numeric(
                scenario_df.get("holding_cost_rate", pd.Series([0.25] * len(scenario_df))),
                errors="coerce",
            ).fillna(0.25)
        ).sum()
        stockout_exposure = scenario_df["stockout_exposure_estimate"].sum()

        scenarios.append(
            {
                "service_level": round(level, 2),
                "safety_stock": round(scenario_df["optimized_safety_stock"].sum(), 2),
                "reorder_point": round(scenario_df["optimized_reorder_point"].sum(), 2),
                "inventory_investment": round(inventory_investment, 2),
                "carrying_cost": round(carrying_cost, 2),
                "stockout_exposure_estimate": round(stockout_exposure, 2),
                "change_vs_current": round(inventory_investment - current_investment, 2),
            }
        )

    return pd.DataFrame(scenarios)
