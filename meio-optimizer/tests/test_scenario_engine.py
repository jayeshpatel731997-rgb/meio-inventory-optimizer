import pandas as pd

from src.scenario_engine import run_service_level_scenarios


def test_scenario_engine_returns_expected_levels_and_monotonic_safety_stock():
    data = pd.DataFrame(
        [
            {
                "sku_id": "SKU001",
                "location_id": "LOC001",
                "avg_demand": 100,
                "std_demand": 20,
                "lead_time": 5,
                "safety_stock": 30,
                "unit_cost": 10,
            }
        ]
    )

    scenario_df = run_service_level_scenarios(data, service_levels=[0.90, 0.95, 0.99])

    assert scenario_df["service_level"].tolist() == [0.9, 0.95, 0.99]
    assert scenario_df["safety_stock"].is_monotonic_increasing
