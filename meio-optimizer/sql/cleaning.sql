-- =============================================================================
-- MEIO OPTIMIZER - cleaning.sql
-- Purpose: Transform raw CSV-shaped staging tables into the typed clean layer
-- Notes:
--   1. Raw tables now mirror the CSV headers exactly.
--   2. This file maps those raw fields into the existing clean schema.
--   3. Where the clean schema expects fields that do not exist in the raw data,
--      we either derive them from raw values or set them to NULL/defaults.
-- =============================================================================

-- Clear clean tables before reload
TRUNCATE TABLE fact_shipments CASCADE;
TRUNCATE TABLE fact_sales_orders CASCADE;
TRUNCATE TABLE fact_inventory_snapshot CASCADE;
TRUNCATE TABLE dim_lane_cost CASCADE;
TRUNCATE TABLE dim_service_policy CASCADE;
TRUNCATE TABLE dim_sku CASCADE;
TRUNCATE TABLE dim_location CASCADE;


-- =============================================================================
-- FIX 1: Load locations
-- Derivations:
--   - echelon comes from location_type (DC=1, RDC=2, STORE=3)
--   - storage_cost_per_unit does not exist in the raw file, so stays NULL
--   - operating_cost_per_day is carried into fixed_operating_cost as the
--     closest existing cost field in the current clean schema
-- =============================================================================
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
    TRIM(location_id) AS location_id,
    TRIM(location_name) AS location_name,
    UPPER(TRIM(location_type)) AS location_type,
    TRIM(region) AS region,
    CASE UPPER(TRIM(location_type))
        WHEN 'DC' THEN 1
        WHEN 'RDC' THEN 2
        WHEN 'STORE' THEN 3
    END AS echelon,
    CAST(NULLIF(capacity_units, '') AS INTEGER) AS capacity_units,
    NULL::NUMERIC AS storage_cost_per_unit,
    CAST(NULLIF(operating_cost_per_day, '') AS NUMERIC) AS fixed_operating_cost
FROM dim_location_raw
WHERE TRIM(location_id) != ''
  AND UPPER(TRIM(location_type)) IN ('DC', 'RDC', 'STORE')
ORDER BY TRIM(location_id), ctid DESC;


-- =============================================================================
-- FIX 2: Load SKUs
-- Derivations:
--   - description comes from sku_name
--   - subcategory and unit_volume_cuft do not exist in raw data, so stay NULL
--   - lead_time_days is imputed from category average when missing
--   - last_updated does not exist in raw data, so stays NULL
-- =============================================================================
WITH category_lead_time_avg AS (
    SELECT
        UPPER(TRIM(category)) AS category,
        ROUND(AVG(CAST(lead_time_days AS NUMERIC))) AS avg_lead_time
    FROM dim_sku_raw
    WHERE lead_time_days IS NOT NULL
      AND lead_time_days != ''
      AND lead_time_days ~ '^[0-9]+(\.[0-9]+)?$'
    GROUP BY UPPER(TRIM(category))
),
sku_prepared AS (
    SELECT
        ctid,
        UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) AS cleaned_sku_id,
        TRIM(sku_name) AS sku_name,
        UPPER(TRIM(category)) AS category,
        unit_cost_usd,
        lead_time_days,
        TRIM(supplier_id) AS supplier_id,
        unit_weight_lbs,
        reorder_point,
        active
    FROM dim_sku_raw
    WHERE sku_id IS NOT NULL
      AND TRIM(sku_id) != ''
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
    p.cleaned_sku_id AS sku_id,
    p.sku_name AS description,
    p.category,
    NULL::VARCHAR(50) AS subcategory,
    CAST(p.unit_cost_usd AS NUMERIC) AS unit_cost,
    NULL::NUMERIC AS unit_volume_cuft,
    CAST(NULLIF(p.unit_weight_lbs, '') AS NUMERIC) AS unit_weight_lbs,
    p.supplier_id,
    COALESCE(
        CAST(NULLIF(p.lead_time_days, '') AS NUMERIC)::INTEGER,
        clt.avg_lead_time::INTEGER,
        7
    ) AS lead_time_days,
    CAST(NULLIF(p.reorder_point, '') AS NUMERIC)::INTEGER AS reorder_point,
    CASE
        WHEN UPPER(TRIM(p.active)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE
        ELSE FALSE
    END AS active,
    NULL::DATE AS last_updated
FROM sku_prepared p
LEFT JOIN category_lead_time_avg clt
    ON p.category = clt.category
WHERE p.unit_cost_usd IS NOT NULL
  AND p.unit_cost_usd != ''
ORDER BY p.cleaned_sku_id, p.ctid DESC;


-- =============================================================================
-- FIX 3: Load service policy
-- Derivations:
--   - target_fill_rate_pct is converted from 99.5-style percentages to 0.995
--   - safety_stock_multiplier maps into z_score
--   - priority_rank is derived from segment
--   - columns not present in raw data stay NULL
-- =============================================================================
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
    UPPER(TRIM(segment)) AS customer_segment,
    CAST(target_fill_rate_pct AS NUMERIC) / 100.0 AS target_fill_rate,
    CAST(safety_stock_multiplier AS NUMERIC) AS z_score,
    CAST(stockout_penalty_per_unit AS NUMERIC) AS penalty_stockout_per_unit,
    CASE UPPER(TRIM(segment))
        WHEN 'PREMIUM' THEN 1
        WHEN 'STANDARD' THEN 2
        WHEN 'BUDGET' THEN 3
    END AS priority_rank,
    CAST(NULLIF(max_lead_time_days, '') AS NUMERIC)::INTEGER AS max_backorder_days,
    NULL::INTEGER AS expedite_threshold,
    CAST(NULLIF(review_period_days, '') AS NUMERIC)::INTEGER AS review_period_days,
    CAST(NULLIF(carrying_cost_rate_annual, '') AS NUMERIC) AS holding_cost_rate,
    NULL::INTEGER AS min_order_qty
FROM dim_service_policy_raw
WHERE UPPER(TRIM(segment)) IN ('PREMIUM', 'STANDARD', 'BUDGET');


-- =============================================================================
-- FIX 4: Load lane costs
-- Derivations:
--   - carrier_mode is mapped into the clean transport_mode field
--   - transit_days_std does not exist in raw data, so stays NULL
-- =============================================================================
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
    TRIM(lane_id) AS lane_id,
    TRIM(origin_location_id) AS from_location,
    TRIM(dest_location_id) AS to_location,
    CASE UPPER(TRIM(carrier_mode))
        WHEN 'TL' THEN 'TRUCK'
        WHEN 'LTL' THEN 'TRUCK'
        WHEN 'PARCEL' THEN 'TRUCK'
        WHEN 'INTERMODAL' THEN 'RAIL'
    END AS transport_mode,
    CAST(NULLIF(distance_miles, '') AS NUMERIC) AS distance_miles,
    CAST(transit_days AS NUMERIC) AS transit_days_mean,
    NULL::NUMERIC AS transit_days_std,
    CAST(cost_per_unit_usd AS NUMERIC) AS cost_per_unit,
    CAST(NULLIF(cost_per_lb_usd, '') AS NUMERIC) AS cost_per_lb
FROM dim_lane_cost_raw
WHERE TRIM(origin_location_id) IN (SELECT location_id FROM dim_location)
  AND TRIM(dest_location_id) IN (SELECT location_id FROM dim_location)
  AND UPPER(TRIM(carrier_mode)) IN ('TL', 'LTL', 'PARCEL', 'INTERMODAL')
ORDER BY TRIM(lane_id), ctid DESC;


-- =============================================================================
-- FIX 5: Load sales orders
-- Derivations:
--   - ship_to_location comes from raw location_id
--   - customer_segment is derived from SKU master segment because the order file
--     does not include a separate customer-segment column
--   - priority_flag does not exist in raw data, so defaults to FALSE
--   - cancelled orders are excluded because the clean fact table is used as a
--     demand signal and does not preserve order_status
-- =============================================================================
WITH sku_segment_lookup AS (
    SELECT DISTINCT ON (cleaned_sku_id)
        cleaned_sku_id,
        UPPER(TRIM(segment)) AS segment
    FROM (
        SELECT
            ctid,
            UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) AS cleaned_sku_id,
            segment
        FROM dim_sku_raw
        WHERE sku_id IS NOT NULL
          AND TRIM(sku_id) != ''
          AND segment IS NOT NULL
          AND TRIM(segment) != ''
    ) s
    ORDER BY cleaned_sku_id, ctid DESC
)
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
SELECT DISTINCT ON (TRIM(r.order_id))
    TRIM(r.order_id) AS order_id,
    CAST(r.order_date AS DATE) AS order_date,
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) AS sku_id,
    TRIM(r.customer_id) AS customer_id,
    TRIM(r.location_id) AS ship_to_location,
    seg.segment AS customer_segment,
    CAST(r.quantity AS NUMERIC)::INTEGER AS qty_ordered,
    CAST(r.unit_price_usd AS NUMERIC) AS price_per_unit,
    TRIM(r.channel) AS channel,
    FALSE AS priority_flag
FROM fact_sales_orders_raw r
LEFT JOIN sku_segment_lookup seg
    ON UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) = seg.cleaned_sku_id
WHERE r.order_id IS NOT NULL
  AND TRIM(r.order_id) != ''
  AND UPPER(COALESCE(TRIM(r.order_status), '')) <> 'CANCELLED'
  AND TRIM(r.location_id) IN (SELECT location_id FROM dim_location)
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', ''))
      IN (SELECT sku_id FROM dim_sku)
  AND seg.segment IN ('PREMIUM', 'STANDARD', 'BUDGET')
  AND r.quantity IS NOT NULL
  AND r.quantity != ''
  AND CAST(r.quantity AS NUMERIC) > 0
  AND r.unit_price_usd IS NOT NULL
  AND r.unit_price_usd != ''
  AND CAST(r.unit_price_usd AS NUMERIC) > 0
ORDER BY TRIM(r.order_id), r.ctid DESC;


-- =============================================================================
-- FIX 6: Load shipments
-- Derivations:
--   - origin_loc_id/dest_loc_id map to from_location/to_location
--   - arrival_date maps to delivery_date
--   - transport_mode is inferred from the lane table's carrier_mode mapping
--   - on_time_flag is derived from actual transit time vs lane transit_days_mean
--   - damage_flag does not exist in raw data, so stays NULL
--   - freight outliers are capped at P99 by inferred transport_mode
--   - NULL freight is imputed with post-cap mode average
--   - inverted arrival dates are nulled out instead of creating negative lead time
-- =============================================================================
WITH lane_lookup AS (
    SELECT DISTINCT ON (from_location, to_location)
        from_location,
        to_location,
        transport_mode,
        transit_days_mean
    FROM dim_lane_cost
    ORDER BY from_location, to_location, transit_days_mean ASC, cost_per_unit ASC, lane_id
),
shipment_prepared AS (
    SELECT
        r.ctid,
        TRIM(r.shipment_id) AS shipment_id,
        NULLIF(TRIM(r.order_id), '') AS order_id,
        TRIM(r.origin_loc_id) AS from_location,
        TRIM(r.dest_loc_id) AS to_location,
        UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) AS sku_id,
        CAST(NULLIF(r.quantity_shipped, '') AS NUMERIC)::INTEGER AS qty_shipped,
        CAST(NULLIF(r.ship_date, '') AS DATE) AS ship_date,
        CAST(NULLIF(r.arrival_date, '') AS DATE) AS raw_delivery_date,
        r.freight_cost_usd,
        TRIM(r.carrier) AS carrier,
        UPPER(TRIM(r.shipment_status)) AS shipment_status,
        ll.transport_mode,
        ll.transit_days_mean
    FROM fact_shipments_raw r
    LEFT JOIN lane_lookup ll
        ON TRIM(r.origin_loc_id) = ll.from_location
       AND TRIM(r.dest_loc_id) = ll.to_location
    WHERE r.shipment_id IS NOT NULL
      AND TRIM(r.shipment_id) != ''
),
shipment_dates_clean AS (
    SELECT
        *,
        CASE
            WHEN raw_delivery_date IS NOT NULL
             AND ship_date IS NOT NULL
             AND raw_delivery_date >= ship_date
                THEN raw_delivery_date
            ELSE NULL::DATE
        END AS delivery_date
    FROM shipment_prepared
),
valid_freight AS (
    SELECT
        transport_mode,
        CAST(freight_cost_usd AS NUMERIC) AS freight_cost
    FROM shipment_dates_clean
    WHERE transport_mode IS NOT NULL
      AND freight_cost_usd IS NOT NULL
      AND freight_cost_usd != ''
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
SELECT DISTINCT ON (s.shipment_id)
    s.shipment_id,
    CASE
        WHEN s.order_id IN (SELECT order_id FROM fact_sales_orders) THEN s.order_id
        ELSE NULL
    END AS order_id,
    s.ship_date,
    s.delivery_date,
    s.from_location,
    s.to_location,
    s.sku_id,
    s.qty_shipped,
    s.transport_mode,
    CASE
        WHEN s.freight_cost_usd IS NULL OR s.freight_cost_usd = ''
            THEN avgf.avg_freight_capped
        WHEN s.freight_cost_usd ~ '^[0-9]+(\.[0-9]+)?$'
         AND p99.p99_threshold IS NOT NULL
         AND CAST(s.freight_cost_usd AS NUMERIC) > p99.p99_threshold
            THEN p99.p99_threshold
        WHEN s.freight_cost_usd ~ '^[0-9]+(\.[0-9]+)?$'
            THEN CAST(s.freight_cost_usd AS NUMERIC)
        ELSE avgf.avg_freight_capped
    END AS freight_cost_usd,
    s.carrier,
    CASE
        WHEN s.delivery_date IS NOT NULL
         AND s.ship_date IS NOT NULL
         AND s.transit_days_mean IS NOT NULL
            THEN (s.delivery_date - s.ship_date) <= s.transit_days_mean
        WHEN s.shipment_status = 'DELAYED'
            THEN FALSE
        ELSE NULL
    END AS on_time_flag,
    NULL::BOOLEAN AS damage_flag
FROM shipment_dates_clean s
LEFT JOIN freight_p99 p99
    ON s.transport_mode = p99.transport_mode
LEFT JOIN freight_avg_post_cap avgf
    ON s.transport_mode = avgf.transport_mode
WHERE s.from_location IN (SELECT location_id FROM dim_location)
  AND s.to_location IN (SELECT location_id FROM dim_location)
  AND s.sku_id IN (SELECT sku_id FROM dim_sku)
  AND s.qty_shipped IS NOT NULL
  AND s.qty_shipped >= 0
ORDER BY s.shipment_id, s.ctid DESC;


-- =============================================================================
-- FIX 7: Load inventory snapshots
-- Derivations:
--   - negative on_hand_qty is floored to 0
--   - backorder_qty does not exist in raw data, so defaults to 0
--   - safety_stock_qty, reorder_point_qty, last_receipt_date, and last_issue_date
--     do not exist in raw data, so stay NULL
-- =============================================================================
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
    CAST(r.snapshot_date AS DATE),
    TRIM(r.location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', ''))
)
    CAST(r.snapshot_date AS DATE) AS snapshot_date,
    TRIM(r.location_id) AS location_id,
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) AS sku_id,
    GREATEST(CAST(r.on_hand_qty AS NUMERIC)::INTEGER, 0) AS on_hand_qty,
    GREATEST(COALESCE(CAST(NULLIF(r.on_order_qty, '') AS NUMERIC)::INTEGER, 0), 0) AS on_order_qty,
    0 AS backorder_qty,
    NULL::INTEGER AS safety_stock_qty,
    NULL::INTEGER AS reorder_point_qty,
    NULL::DATE AS last_receipt_date,
    NULL::DATE AS last_issue_date
FROM fact_inventory_snapshot_raw r
WHERE r.snapshot_date IS NOT NULL
  AND r.snapshot_date != ''
  AND TRIM(r.location_id) IN (SELECT location_id FROM dim_location)
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', ''))
      IN (SELECT sku_id FROM dim_sku)
ORDER BY
    CAST(r.snapshot_date AS DATE),
    TRIM(r.location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')),
    r.ctid DESC;


-- =============================================================================
-- FINAL: Simple audit summary
-- =============================================================================
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
