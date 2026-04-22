-- =============================================================================
-- MEIO OPTIMIZER - ingest.sql
-- Purpose: Load all 7 raw CSVs into staging tables (raw_* layer)
-- IMPORTANT: Run schema.sql first.
-- IMPORTANT: Raw staging tables mirror the CSV headers exactly.
-- =============================================================================

-- =============================================================================
-- STEP 0: Clear raw tables before reload
-- =============================================================================
TRUNCATE TABLE dim_sku_raw CASCADE;
TRUNCATE TABLE dim_location_raw CASCADE;
TRUNCATE TABLE fact_sales_orders_raw CASCADE;
TRUNCATE TABLE fact_shipments_raw CASCADE;
TRUNCATE TABLE fact_inventory_snapshot_raw CASCADE;
TRUNCATE TABLE dim_lane_cost_raw CASCADE;
TRUNCATE TABLE dim_service_policy_raw CASCADE;


-- =============================================================================
-- STEP 1: Load dimension-style raw tables
-- =============================================================================

COPY dim_location_raw (
    location_id,
    location_name,
    city,
    state,
    location_type,
    region,
    capacity_units,
    operating_cost_per_day
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/locations.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

COPY dim_sku_raw (
    sku_id,
    sku_name,
    category,
    unit_cost_usd,
    lead_time_days,
    supplier_id,
    unit_weight_lbs,
    shelf_life_days,
    reorder_point,
    min_order_qty,
    segment,
    active
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/sku_master.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

COPY dim_service_policy_raw (
    segment,
    target_fill_rate_pct,
    target_cycle_service_lvl,
    max_lead_time_days,
    safety_stock_multiplier,
    review_period_days,
    order_up_to_days,
    carrying_cost_rate_annual,
    stockout_penalty_per_unit,
    notes
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/service_policy.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

COPY dim_lane_cost_raw (
    lane_id,
    origin_location_id,
    dest_location_id,
    distance_miles,
    transit_days,
    cost_per_unit_usd,
    cost_per_lb_usd,
    carrier_mode,
    contract_type
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/lane_costs.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');


-- =============================================================================
-- STEP 2: Load fact-style raw tables
-- =============================================================================

COPY fact_sales_orders_raw (
    order_id,
    order_date,
    customer_id,
    sku_id,
    location_id,
    quantity,
    unit_price_usd,
    order_status,
    channel,
    promised_delivery_date
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/sales_orders.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

COPY fact_shipments_raw (
    shipment_id,
    order_id,
    origin_loc_id,
    dest_loc_id,
    sku_id,
    quantity_shipped,
    ship_date,
    arrival_date,
    freight_cost_usd,
    carrier,
    shipment_status,
    weight_lbs,
    pallet_count
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/shipments.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');

COPY fact_inventory_snapshot_raw (
    snapshot_id,
    snapshot_date,
    location_id,
    sku_id,
    on_hand_qty,
    on_order_qty,
    reserved_qty,
    available_qty,
    snapshot_source,
    days_of_supply
)
FROM 'C:/Users/jayes/Desktop/MEIO/meio-optimizer/data/raw/inventory_snapshots.csv'
WITH (FORMAT CSV, HEADER TRUE, NULL '');


-- =============================================================================
-- STEP 3: Post-ingest row count summary
-- =============================================================================
SELECT 'dim_location_raw' AS table_name, COUNT(*) AS rows_loaded FROM dim_location_raw
UNION ALL
SELECT 'dim_sku_raw', COUNT(*) FROM dim_sku_raw
UNION ALL
SELECT 'dim_service_policy_raw', COUNT(*) FROM dim_service_policy_raw
UNION ALL
SELECT 'dim_lane_cost_raw', COUNT(*) FROM dim_lane_cost_raw
UNION ALL
SELECT 'fact_sales_orders_raw', COUNT(*) FROM fact_sales_orders_raw
UNION ALL
SELECT 'fact_shipments_raw', COUNT(*) FROM fact_shipments_raw
UNION ALL
SELECT 'fact_inventory_snapshot_raw', COUNT(*) FROM fact_inventory_snapshot_raw
ORDER BY table_name;
