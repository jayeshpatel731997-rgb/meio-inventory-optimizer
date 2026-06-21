import pandas as pd

from src.recommendations import generate_recommendations


def test_generate_recommendations_returns_actions():
    policy_df = pd.DataFrame(
        [
            {
                "sku_id": "SKU001",
                "location_id": "LOC001",
                "inventory_position": 50,
                "optimized_reorder_point": 120,
                "optimized_safety_stock": 30,
                "safety_stock": 10,
                "days_of_supply": 4,
                "unit_cost": 12,
                "observation_days": 40,
                "std_demand": 25,
                "avg_demand": 20,
            }
        ]
    )

    recommendations_df = generate_recommendations(policy_df)

    assert not recommendations_df.empty
    assert recommendations_df.iloc[0]["priority"] == "Critical"
    assert recommendations_df.iloc[0]["sku"] == "SKU001"
