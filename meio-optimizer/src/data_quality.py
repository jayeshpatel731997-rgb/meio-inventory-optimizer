import pandas as pd


def _issue_count(series_or_value) -> int:
    if isinstance(series_or_value, pd.Series):
        return int(series_or_value.sum())
    return int(series_or_value)


def run_data_quality_checks(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            [
                {"check_name": "dataset_empty", "issue_count": 1, "status": "WARN", "detail": "No rows available."}
            ]
        )

    checks = []

    checks.append(
        {
            "check_name": "missing_sku",
            "issue_count": _issue_count(df["sku_id"].isna() | (df["sku_id"].astype(str).str.strip() == "")),
            "status": "PASS",
            "detail": "Missing or blank sku_id values.",
        }
    )
    checks.append(
        {
            "check_name": "missing_location",
            "issue_count": _issue_count(df["location_id"].isna() | (df["location_id"].astype(str).str.strip() == "")),
            "status": "PASS",
            "detail": "Missing or blank location_id values.",
        }
    )
    checks.append(
        {
            "check_name": "negative_inventory",
            "issue_count": _issue_count(pd.to_numeric(df.get("on_hand_qty"), errors="coerce").fillna(0) < 0),
            "status": "PASS",
            "detail": "Negative on-hand inventory.",
        }
    )
    checks.append(
        {
            "check_name": "missing_demand",
            "issue_count": _issue_count(pd.to_numeric(df.get("avg_demand"), errors="coerce").isna()),
            "status": "PASS",
            "detail": "Missing average demand values.",
        }
    )
    checks.append(
        {
            "check_name": "missing_lead_time",
            "issue_count": _issue_count(pd.to_numeric(df.get("lead_time"), errors="coerce").isna()),
            "status": "PASS",
            "detail": "Missing lead time values.",
        }
    )
    checks.append(
        {
            "check_name": "duplicate_sku_location",
            "issue_count": int(df.duplicated(subset=["sku_id", "location_id"]).sum()),
            "status": "PASS",
            "detail": "Duplicate sku-location combinations.",
        }
    )
    checks.append(
        {
            "check_name": "zero_demand",
            "issue_count": _issue_count(
                pd.to_numeric(df.get("avg_demand"), errors="coerce").fillna(0).eq(0)
            ),
            "status": "PASS",
            "detail": "Rows with zero average demand.",
        }
    )
    checks.append(
        {
            "check_name": "missing_unit_cost",
            "issue_count": _issue_count(pd.to_numeric(df.get("unit_cost"), errors="coerce").isna()),
            "status": "PASS",
            "detail": "Missing unit cost values.",
        }
    )

    checks_df = pd.DataFrame(checks)
    checks_df.loc[checks_df["issue_count"] > 0, "status"] = "WARN"
    return checks_df
