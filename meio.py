import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# =========================
# DB CONNECTION
# =========================
engine = create_engine("postgresql://postgres:Jayesh%4073@localhost:5432/meio_optimizer_db")

# =========================
# LOAD DATA
# =========================
demand_df = pd.read_sql("SELECT * FROM mart_demand_stats", engine)
flow_df = pd.read_sql("SELECT * FROM mart_network_flow", engine)

# =========================
# PREP DEMAND
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

std_demand=("avg_demand_per_day", "std")

# =========================
# PREP LEAD TIME (FAKE FOR NOW)
# =========================

# since you don't have lead time → simulate based on lanes
lead_time = flow_df[["sku_id", "to_location"]].drop_duplicates().copy()

lead_time["avg_lead_time"] = np.random.randint(3, 10, size=len(lead_time))

# match column name
lead_time.rename(columns={"to_location": "location_id"}, inplace=True)

# =========================
# MERGE
# =========================
df = agg_demand.merge(lead_time, on=["sku_id", "location_id"], how="inner")

df["std_demand"] = df["std_demand"].fillna(0)

# =========================
# MONTE CARLO (FAST VERSION)
# =========================

SIMULATIONS = 2000
SERVICE_TARGET = 0.95

results = []

for _, row in df.iterrows():
    mean = row["avg_demand"]
    std = max(row["std_demand"], 0.3 * mean)
    lead_time_days = row["avg_lead_time"]

    if mean <= 0:
        continue

    # ⚡ FAST vectorized simulation
    samples = np.random.normal(
        mean,
        std,
        (SIMULATIONS, int(np.ceil(lead_time_days)))
    )

    samples = np.maximum(samples, 0)
    demand_samples = samples.sum(axis=1)

    reorder_point = np.percentile(demand_samples, SERVICE_TARGET * 100)
    safety_stock = reorder_point - (mean * lead_time_days)

    results.append({
        "sku_id": row["sku_id"],
        "location_id": row["location_id"],
        "avg_demand": round(mean, 2),
        "std_demand": round(std, 2),
        "lead_time": lead_time_days,
        "reorder_point": round(reorder_point, 2),
        "safety_stock": round(safety_stock, 2)
    })

# =========================
# OUTPUT
# =========================
result_df = pd.DataFrame(results)

print(result_df.head())

result_df.to_sql("opt_inventory_policy", engine, if_exists="replace", index=False)

print("✅ DONE — opt_inventory_policy created")