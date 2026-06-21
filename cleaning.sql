-- =============================================================================
-- MEIO OPTIMIZER - cleaning.sql
-- Purpose: Transform raw staging tables into typed production tables.
-- =============================================================================

TRUNCATE TABLE fact_shipments CASCADE;
TRUNCATE TABLE fact_sales_orders CASCADE;
TRUNCATE TABLE fact_inventory_snapshot CASCADE;
TRUNCATE TABLE dim_lane_cost CASCADE;
TRUNCATE TABLE dim_service_policy CASCADE;
TRUNCATE TABLE dim_sku CASCADE;
TRUNCATE TABLE dim_location CASCADE;

INSERT INTO dim_location (
    location_id,
    location_name,
    location_type,
    region,
    echelon,
    capacity_units,
    storage_cost_per_unit,
    fixed_operating_cost
)
SELECT DISTINCT ON (TRIM(location_id))
    TRIM(location_id),
    TRIM(location_name),
    UPPER(TRIM(location_type)),
    TRIM(region),
    CAST(echelon AS integer),
    CAST(NULLIF(capacity_units, '') AS integer),
    CAST(NULLIF(storage_cost_per_unit, '') AS numeric),
    CAST(NULLIF(fixed_operating_cost, '') AS numeric)
FROM dim_location_raw
WHERE NULLIF(TRIM(location_id), '') IS NOT NULL
  AND UPPER(TRIM(location_type)) IN ('DC', 'RDC', 'STORE')
ORDER BY TRIM(location_id), ctid DESC;

WITH category_lead_time_avg AS (
    SELECT
        UPPER(TRIM(category)) AS category,
        ROUND(AVG(CAST(lead_time_days AS numeric))) AS avg_lead_time
    FROM dim_sku_raw
    WHERE NULLIF(TRIM(lead_time_days), '') IS NOT NULL
      AND lead_time_days ~ '^[0-9]+(\.[0-9]+)?$'
    GROUP BY UPPER(TRIM(category))
),
sku_prepared AS (
    SELECT
        ctid,
        UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) AS cleaned_sku_id,
        TRIM(description) AS description,
        UPPER(TRIM(category)) AS category,
        NULLIF(TRIM(subcategory), '') AS subcategory,
        unit_cost,
        unit_volume_cuft,
        unit_weight_lbs,
        TRIM(supplier_id) AS supplier_id,
        lead_time_days,
        reorder_point,
        active,
        last_updated
    FROM dim_sku_raw
    WHERE NULLIF(TRIM(sku_id), '') IS NOT NULL
)
INSERT INTO dim_sku (
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
SELECT DISTINCT ON (p.cleaned_sku_id)
    p.cleaned_sku_id,
    p.description,
    p.category,
    p.subcategory,
    CAST(p.unit_cost AS numeric),
    CAST(NULLIF(p.unit_volume_cuft, '') AS numeric),
    CAST(NULLIF(p.unit_weight_lbs, '') AS numeric),
    p.supplier_id,
    COALESCE(
        CAST(NULLIF(p.lead_time_days, '') AS numeric)::integer,
        clt.avg_lead_time::integer,
        7
    ),
    CAST(NULLIF(p.reorder_point, '') AS numeric)::integer,
    CASE
        WHEN UPPER(TRIM(p.active)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE
        ELSE FALSE
    END,
    CAST(NULLIF(p.last_updated, '') AS date)
FROM sku_prepared p
LEFT JOIN category_lead_time_avg clt
    ON p.category = clt.category
WHERE p.cleaned_sku_id != ''
ORDER BY p.cleaned_sku_id, p.ctid DESC;

INSERT INTO dim_service_policy (
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
SELECT
    UPPER(TRIM(customer_segment)),
    CAST(target_fill_rate AS numeric),
    CAST(z_score AS numeric),
    CAST(penalty_stockout_per_unit AS numeric),
    CAST(priority_rank AS integer),
    CAST(NULLIF(max_backorder_days, '') AS integer),
    CAST(NULLIF(expedite_threshold, '') AS integer),
    CAST(NULLIF(review_period_days, '') AS integer),
    CAST(NULLIF(holding_cost_rate, '') AS numeric),
    CAST(NULLIF(min_order_qty, '') AS integer)
FROM dim_service_policy_raw
WHERE UPPER(TRIM(customer_segment)) IN ('PREMIUM', 'STANDARD', 'BUDGET');

INSERT INTO dim_lane_cost (
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
SELECT DISTINCT ON (TRIM(lane_id))
    TRIM(lane_id),
    TRIM(from_location),
    TRIM(to_location),
    UPPER(TRIM(transport_mode)),
    CAST(NULLIF(distance_miles, '') AS numeric),
    CAST(transit_days_mean AS numeric),
    CAST(NULLIF(transit_days_std, '') AS numeric),
    CAST(cost_per_unit AS numeric),
    CAST(NULLIF(cost_per_lb, '') AS numeric)
FROM dim_lane_cost_raw
WHERE TRIM(from_location) IN (SELECT location_id FROM dim_location)
  AND TRIM(to_location) IN (SELECT location_id FROM dim_location)
  AND UPPER(TRIM(transport_mode)) IN ('TRUCK', 'AIR', 'RAIL')
ORDER BY TRIM(lane_id), ctid DESC;

INSERT INTO fact_sales_orders (
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
SELECT DISTINCT ON (TRIM(order_id))
    TRIM(order_id),
    CAST(order_date AS date),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')),
    TRIM(customer_id),
    NULLIF(TRIM(ship_to_location), ''),
    UPPER(TRIM(customer_segment)),
    CAST(qty_ordered AS integer),
    CAST(price_per_unit AS numeric),
    TRIM(channel),
    CASE WHEN UPPER(TRIM(priority_flag)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE ELSE FALSE END
FROM fact_sales_orders_raw
WHERE NULLIF(TRIM(order_id), '') IS NOT NULL
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) IN (SELECT sku_id FROM dim_sku)
  AND NULLIF(TRIM(ship_to_location), '') IN (SELECT location_id FROM dim_location)
  AND UPPER(TRIM(customer_segment)) IN (SELECT customer_segment FROM dim_service_policy)
ORDER BY TRIM(order_id), ctid DESC;

WITH valid_freight AS (
    SELECT
        UPPER(TRIM(transport_mode)) AS transport_mode,
        CAST(freight_cost_usd AS numeric) AS freight_cost
    FROM fact_shipments_raw
    WHERE NULLIF(TRIM(freight_cost_usd), '') IS NOT NULL
      AND freight_cost_usd ~ '^[0-9]+(\.[0-9]+)?$'
),
freight_p99 AS (
    SELECT
        transport_mode,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY freight_cost) AS p99_threshold
    FROM valid_freight
    GROUP BY transport_mode
),
freight_avg_post_cap AS (
    SELECT
        v.transport_mode,
        AVG(LEAST(v.freight_cost, p.p99_threshold)) AS avg_freight_capped
    FROM valid_freight v
    JOIN freight_p99 p
        ON v.transport_mode = p.transport_mode
    GROUP BY v.transport_mode
)
INSERT INTO fact_shipments (
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
SELECT DISTINCT ON (TRIM(r.shipment_id))
    TRIM(r.shipment_id),
    NULLIF(TRIM(r.order_id), ''),
    CAST(NULLIF(r.ship_date, '') AS date),
    CAST(NULLIF(r.delivery_date, '') AS date),
    NULLIF(TRIM(r.from_location), ''),
    NULLIF(TRIM(r.to_location), ''),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')),
    CAST(NULLIF(r.qty_shipped, '') AS integer),
    UPPER(TRIM(r.transport_mode)),
    COALESCE(
        CASE
            WHEN NULLIF(TRIM(r.freight_cost_usd), '') IS NULL THEN avg.avg_freight_capped
            WHEN CAST(r.freight_cost_usd AS numeric) > p99.p99_threshold THEN p99.p99_threshold
            ELSE CAST(r.freight_cost_usd AS numeric)
        END,
        0
    ),
    TRIM(r.carrier),
    CASE WHEN UPPER(TRIM(r.on_time_flag)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE ELSE FALSE END,
    CASE WHEN UPPER(TRIM(r.damage_flag)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE ELSE FALSE END
FROM fact_shipments_raw r
LEFT JOIN freight_p99 p99
    ON UPPER(TRIM(r.transport_mode)) = p99.transport_mode
LEFT JOIN freight_avg_post_cap avg
    ON UPPER(TRIM(r.transport_mode)) = avg.transport_mode
WHERE NULLIF(TRIM(r.shipment_id), '') IS NOT NULL
  AND (NULLIF(TRIM(r.order_id), '') IS NULL OR NULLIF(TRIM(r.order_id), '') IN (SELECT order_id FROM fact_sales_orders))
  AND NULLIF(TRIM(r.from_location), '') IN (SELECT location_id FROM dim_location)
  AND NULLIF(TRIM(r.to_location), '') IN (SELECT location_id FROM dim_location)
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) IN (SELECT sku_id FROM dim_sku)
  AND UPPER(TRIM(r.transport_mode)) IN ('TRUCK', 'AIR', 'RAIL')
ORDER BY TRIM(r.shipment_id), r.ctid DESC;

INSERT INTO fact_inventory_snapshot (
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
SELECT DISTINCT ON (
    CAST(snapshot_date AS date),
    TRIM(location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''))
)
    CAST(snapshot_date AS date),
    TRIM(location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')),
    GREATEST(CAST(on_hand_qty AS integer), 0),
    GREATEST(COALESCE(CAST(NULLIF(on_order_qty, '') AS integer), 0), 0),
    GREATEST(COALESCE(CAST(NULLIF(backorder_qty, '') AS integer), 0), 0),
    CAST(NULLIF(safety_stock_qty, '') AS integer),
    CAST(NULLIF(reorder_point_qty, '') AS integer),
    CAST(NULLIF(last_receipt_date, '') AS date),
    CAST(NULLIF(last_issue_date, '') AS date)
FROM fact_inventory_snapshot_raw
WHERE NULLIF(TRIM(snapshot_date), '') IS NOT NULL
  AND TRIM(location_id) IN (SELECT location_id FROM dim_location)
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) IN (SELECT sku_id FROM dim_sku)
ORDER BY
    CAST(snapshot_date AS date),
    TRIM(location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')),
    ctid DESC;

SELECT 'dim_location' AS table_name, COUNT(*) AS rows_loaded FROM dim_location
UNION ALL
SELECT 'dim_sku', COUNT(*) FROM dim_sku
UNION ALL
SELECT 'dim_service_policy', COUNT(*) FROM dim_service_policy
UNION ALL
SELECT 'dim_lane_cost', COUNT(*) FROM dim_lane_cost
UNION ALL
SELECT 'fact_sales_orders', COUNT(*) FROM fact_sales_orders
UNION ALL
SELECT 'fact_shipments', COUNT(*) FROM fact_shipments
UNION ALL
SELECT 'fact_inventory_snapshot', COUNT(*) FROM fact_inventory_snapshot
ORDER BY table_name;
