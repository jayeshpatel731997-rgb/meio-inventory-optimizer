-- =============================================================================
-- MEIO OPTIMIZER - ingest.sql
-- Purpose: Load all raw CSVs into staging tables using psql client-side \copy.
-- CSV paths are supplied by run_pipeline.ps1 through psql variables. This keeps
-- the same ingest script portable across Windows, Linux, and CI runners.
--
-- IMPORTANT:
-- Run this file with psql or run_pipeline.ps1.
-- pgAdmin does not execute psql meta-commands like \copy.
-- =============================================================================

TRUNCATE TABLE dim_sku_raw CASCADE;
TRUNCATE TABLE dim_location_raw CASCADE;
TRUNCATE TABLE fact_sales_orders_raw CASCADE;
TRUNCATE TABLE fact_shipments_raw CASCADE;
TRUNCATE TABLE fact_inventory_snapshot_raw CASCADE;
TRUNCATE TABLE dim_lane_cost_raw CASCADE;
TRUNCATE TABLE dim_service_policy_raw CASCADE;

\copy dim_location_raw (location_id, location_name, location_type, region, echelon, capacity_units, storage_cost_per_unit, fixed_operating_cost) FROM '{{locations_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy dim_sku_raw (sku_id, description, category, subcategory, unit_cost, unit_volume_cuft, unit_weight_lbs, supplier_id, lead_time_days, reorder_point, active, last_updated) FROM '{{sku_master_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy dim_service_policy_raw (customer_segment, target_fill_rate, z_score, penalty_stockout_per_unit, priority_rank, max_backorder_days, expedite_threshold, review_period_days, holding_cost_rate, min_order_qty) FROM '{{service_policy_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy dim_lane_cost_raw (lane_id, from_location, to_location, transport_mode, distance_miles, transit_days_mean, transit_days_std, cost_per_unit, cost_per_lb) FROM '{{lane_costs_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy fact_sales_orders_raw (order_id, order_date, sku_id, customer_id, ship_to_location, customer_segment, qty_ordered, price_per_unit, channel, priority_flag) FROM '{{sales_orders_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy fact_shipments_raw (shipment_id, order_id, ship_date, delivery_date, from_location, to_location, sku_id, qty_shipped, transport_mode, freight_cost_usd, carrier, on_time_flag, damage_flag) FROM '{{shipments_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

\copy fact_inventory_snapshot_raw (snapshot_date, location_id, sku_id, on_hand_qty, on_order_qty, backorder_qty, safety_stock_qty, reorder_point_qty, last_receipt_date, last_issue_date) FROM '{{inventory_snapshots_csv}}' WITH (FORMAT csv, HEADER true, NULL '');

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
