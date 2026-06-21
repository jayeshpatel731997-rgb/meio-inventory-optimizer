import pandas as pd

from src.data_quality import run_data_quality_checks


def test_data_quality_flags_expected_issues():
    df = pd.DataFrame(
        [
            {
                "sku_id": "SKU001",
                "location_id": "LOC001",
                "on_hand_qty": -1,
                "avg_demand": 0,
                "lead_time": 5,
                "unit_cost": None,
            },
            {
                "sku_id": "SKU001",
                "location_id": "LOC001",
                "on_hand_qty": 5,
                "avg_demand": None,
                "lead_time": None,
                "unit_cost": 10,
            },
            {
                "sku_id": None,
                "location_id": "",
                "on_hand_qty": 10,
                "avg_demand": 10,
                "lead_time": 3,
                "unit_cost": 5,
            },
        ]
    )

    checks_df = run_data_quality_checks(df)
    issues = dict(zip(checks_df["check_name"], checks_df["issue_count"]))

    assert issues["missing_sku"] == 1
    assert issues["missing_location"] == 1
    assert issues["negative_inventory"] == 1
    assert issues["duplicate_sku_location"] == 1
