-- =============================================================================
-- MEIO OPTIMIZER — ingest.sql
-- Purpose: Load all 7 raw CSVs into staging tables (raw_* layer)
-- IMPORTANT: Run schema.sql first. These tables must exist before COPY runs.
-- IMPORTANT: Update the file paths below to match your actual folder location.
--
-- Why load into raw tables first (not directly into clean tables)?
-- Because COPY fails on the first type mismatch. If freight_cost has '$297,072'
-- and the column is NUMERIC, the entire file import aborts. Raw tables accept
-- TEXT for everything — we convert types in cleaning.sql where we can handle
-- bad values row-by-row instead of failing on the first bad cell.
-- =============================================================================

-- =============================================================================
-- STEP 0: Clear raw tables before reload (idempotent — safe to re-run)
-- =============================================================================
TRUNCATE TABLE dim_sku_raw CASCADE;
TRUNCATE TABLE dim_location_raw CASCADE;
TRUNCATE TABLE fact_sales_orders_raw CASCADE;
TRUNCATE TABLE fact_shipments_raw CASCADE;
TRUNCATE TABLE fact_inventory_snapshot_raw CASCADE;
TRUNCATE TABLE dim_lane_cost_raw CASCADE;
TRUNCATE TABLE dim_service_policy_raw CASCADE;


-- =============================================================================
-- STEP 1: Load anchor tables first (everything joins to these)
-- =============================================================================

-- Load locations (28 rows, clean anchor table)
-- Why first: Every fact table has FKs to location_id. Load this before orders/shipments.
COPY dim_location_raw (
    location_id,
    location_name,
    location_type,
    region,
    echelon,
    capacity_units,
    storage_cost_per_unit,
    fixed_operating_cost
)
FROM '/meio-optimizer/data/raw/locations.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: should be 28 rows
-- SELECT COUNT(*), location_type, COUNT(*) FROM dim_location_raw GROUP BY location_type;
-- Expected: DC=6, RDC=10, STORE=12


-- Load SKU master (200 rows, messy sku_id formats, 24 NULL lead_time_days)
-- Why second: Sales orders and shipments join on sku_id.
COPY dim_sku_raw (
    sku_id,
    description,
    category,
    subcategory,
    unit_cost,
    unit_volume_cuft,
    unit_weight_lbs,
    supplier_id,
    lead_time_days,
    reorder_point,
    active,
    last_updated
)
FROM '/meio-optimizer/data/raw/sku_master.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: should be 200 rows with mixed sku_id formats
-- SELECT LEFT(sku_id, 6), COUNT(*) FROM dim_sku_raw GROUP BY LEFT(sku_id, 6);
-- You should see 5 distinct formats: SKU-0, SKU00, sku_0, Sku-0, SKU 0


-- Load service policy (3 rows — PREMIUM, STANDARD, BUDGET)
-- Load early: cleaning.sql references z_score values from here
COPY dim_service_policy_raw (
    customer_segment,
    target_fill_rate,
    z_score,
    penalty_stockout_per_unit,
    priority_rank,
    max_backorder_days,
    expedite_threshold,
    review_period_days,
    holding_cost_rate,
    min_order_qty
)
FROM '/meio-optimizer/data/raw/service_policy.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: exactly 3 rows
-- SELECT customer_segment, z_score, target_fill_rate FROM dim_service_policy_raw;
-- Expected: PREMIUM z=2.33, STANDARD z=1.65, BUDGET z=1.28


-- Load lane costs (140 rows — DC→RDC and RDC→STORE lanes)
COPY dim_lane_cost_raw (
    lane_id,
    from_location,
    to_location,
    transport_mode,
    distance_miles,
    transit_days_mean,
    transit_days_std,
    cost_per_unit,
    cost_per_lb
)
FROM '/meio-optimizer/data/raw/lane_costs.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: should be 140 rows
-- SELECT transport_mode, COUNT(*) FROM dim_lane_cost_raw GROUP BY transport_mode;


-- =============================================================================
-- STEP 2: Load fact tables (depend on dim tables above)
-- =============================================================================

-- Load sales orders (500 rows — includes ~10 duplicate order_ids)
-- Note: We load ALL rows including duplicates — deduplication happens in cleaning.sql
-- Why not dedupe here? We want to log HOW MANY duplicates we found, not silently drop them.
COPY fact_sales_orders_raw (
    order_id,
    order_date,
    sku_id,
    customer_id,
    ship_to_location,
    customer_segment,
    qty_ordered,
    price_per_unit,
    channel,
    priority_flag
)
FROM '/meio-optimizer/data/raw/sales_orders.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: should be 500 rows (duplicates intentionally kept until cleaning.sql)
-- SELECT COUNT(*) as total, COUNT(DISTINCT order_id) as unique_orders FROM fact_sales_orders_raw;
-- Expected: total=500, unique_orders=490 (10 duplicates)


-- Load shipments (400 rows — 30 NULL freight, 7 freight outliers)
-- Note: freight_cost_usd is TEXT in raw table so '$297,072' doesn't crash the import
COPY fact_shipments_raw (
    shipment_id,
    order_id,
    ship_date,
    delivery_date,
    from_location,
    to_location,
    sku_id,
    qty_shipped,
    transport_mode,
    freight_cost_usd,
    carrier,
    on_time_flag,
    damage_flag
)
FROM '/meio-optimizer/data/raw/shipments.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: 400 rows, check outlier count
-- SELECT COUNT(*) as total,
--        SUM(CASE WHEN freight_cost_usd IS NULL THEN 1 ELSE 0 END) as null_freight,
--        SUM(CASE WHEN freight_cost_usd::NUMERIC > 10000 THEN 1 ELSE 0 END) as outlier_freight
-- FROM fact_shipments_raw;
-- Expected: total=400, null_freight=30, outlier_freight=7


-- Load inventory snapshots (300 rows — 9 rows with negative on_hand_qty)
COPY fact_inventory_snapshot_raw (
    snapshot_date,
    location_id,
    sku_id,
    on_hand_qty,
    on_order_qty,
    backorder_qty,
    safety_stock_qty,
    reorder_point_qty,
    last_receipt_date,
    last_issue_date
)
FROM '/meio-optimizer/data/raw/inventory_snapshots.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

-- Verify: 300 rows, check negative inventory count
-- SELECT COUNT(*) as total,
--        SUM(CASE WHEN on_hand_qty::INTEGER < 0 THEN 1 ELSE 0 END) as negative_inventory
-- FROM fact_inventory_snapshot_raw;
-- Expected: total=300, negative_inventory=9


-- =============================================================================
-- STEP 3: Post-ingest row count summary
-- Run this block to confirm all 7 tables loaded correctly before running cleaning.sql
-- =============================================================================
SELECT 'dim_location_raw'           AS table_name, COUNT(*) AS rows_loaded FROM dim_location_raw
UNION ALL
SELECT 'dim_sku_raw',                               COUNT(*) FROM dim_sku_raw
UNION ALL
SELECT 'dim_service_policy_raw',                    COUNT(*) FROM dim_service_policy_raw
UNION ALL
SELECT 'dim_lane_cost_raw',                         COUNT(*) FROM dim_lane_cost_raw
UNION ALL
SELECT 'fact_sales_orders_raw',                     COUNT(*) FROM fact_sales_orders_raw
UNION ALL
SELECT 'fact_shipments_raw',                        COUNT(*) FROM fact_shipments_raw
UNION ALL
SELECT 'fact_inventory_snapshot_raw',               COUNT(*) FROM fact_inventory_snapshot_raw
ORDER BY table_name;

-- Expected output:
-- dim_lane_cost_raw            | 140
-- dim_location_raw             |  28
-- dim_service_policy_raw       |   3
-- dim_sku_raw                  | 200
-- fact_inventory_snapshot_raw  | 300
-- fact_sales_orders_raw        | 500
-- fact_shipments_raw           | 400
