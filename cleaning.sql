-- =============================================================================
-- MEIO OPTIMIZER — cleaning.sql
-- Purpose: Fix all injected data quality issues and load clean production tables
-- This file is the portfolio differentiator — it shows SQL judgment, not just syntax.
--
-- PROBLEMS WE'RE FIXING (each documented with WHY the fix is what it is):
--   1. Mixed SKU ID formats (SKU-001, SKU001, sku_001, Sku-001, SKU 001)
--   2. Missing lead_time_days (~12% NULL) — imputed from category average
--   3. Inconsistent active flag values (Y, YES, y, 1, true)
--   4. Duplicate order_ids (~2% of orders) — ERP re-transmission bug
--   5. Freight cost outliers (7 rows at 100x normal) — capped at P99
--   6. NULL freight costs (30 rows) — imputed from mode average
--   7. Negative on_hand_qty (9 rows) — floored to 0
-- =============================================================================

-- Clear clean tables before reload (idempotent)
TRUNCATE TABLE fact_shipments CASCADE;
TRUNCATE TABLE fact_sales_orders CASCADE;
TRUNCATE TABLE fact_inventory_snapshot CASCADE;
TRUNCATE TABLE dim_lane_cost CASCADE;
TRUNCATE TABLE dim_service_policy CASCADE;
TRUNCATE TABLE dim_sku CASCADE;
TRUNCATE TABLE dim_location CASCADE;


-- =============================================================================
-- FIX 1: Standardize location data (anchor table — fix first, everything else joins here)
-- No quality issues injected, but we normalize types before FK constraints apply
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
SELECT
    TRIM(location_id),
    TRIM(location_name),
    UPPER(TRIM(location_type)),             -- normalize to DC / RDC / STORE
    TRIM(region),
    CAST(echelon AS INTEGER),
    CAST(NULLIF(capacity_units, '') AS INTEGER),
    CAST(NULLIF(storage_cost_per_unit, '') AS NUMERIC),
    CAST(NULLIF(fixed_operating_cost, '') AS NUMERIC)
FROM dim_location_raw
WHERE location_id IS NOT NULL
  AND location_id != '';

-- Verify: 28 rows, 3 distinct types
-- SELECT location_type, COUNT(*) FROM dim_location GROUP BY location_type;


-- =============================================================================
-- FIX 2: Standardize SKU IDs + impute missing lead times + normalize active flag
--
-- WHY this SKU standardization approach:
--   Goal: make 'SKU-001', 'SKU001', 'sku_001', 'Sku-001', 'SKU 001' all resolve to 'SKU001'
--   Method: UPPER() + strip hyphens + strip underscores + strip spaces
--   Order matters: strip spaces last, otherwise 'SKU 001' → 'SKU001' needs the space gone first
--
-- WHY category-average imputation for lead_time_days (not global average):
--   A NULL lead time for an Electronics SKU should use Electronics avg (~7 days)
--   not the global avg which includes Bulk materials (~21 days).
--   Using a global average would underestimate safety stock for slow-supplier categories
--   and overestimate it for fast-supplier categories — both wrong in opposite directions.
--
-- WHY ROUND() for lead time:
--   Lead time drives the safety stock formula: SS = z * σ_d * sqrt(L)
--   Fractional lead times are mathematically valid but operationally meaningless —
--   you can't place an order for 7.3 days. Round to nearest integer.
-- =============================================================================

-- First: compute category-average lead times from non-NULL rows
-- We'll use this as a CTE inside the INSERT
WITH category_lead_time_avg AS (
    SELECT
        UPPER(TRIM(category)) AS category,
        ROUND(AVG(CAST(lead_time_days AS NUMERIC))) AS avg_lead_time
    FROM dim_sku_raw
    WHERE lead_time_days IS NOT NULL
      AND lead_time_days != ''
      AND lead_time_days ~ '^[0-9]+(\.[0-9]+)?$'  -- only rows with valid numeric values
    GROUP BY UPPER(TRIM(category))
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
SELECT DISTINCT ON (cleaned_sku_id)
    cleaned_sku_id,
    TRIM(r.description),
    UPPER(TRIM(r.category)),
    TRIM(r.subcategory),
    CAST(r.unit_cost AS NUMERIC),
    CAST(NULLIF(r.unit_volume_cuft, '') AS NUMERIC),
    CAST(NULLIF(r.unit_weight_lbs, '') AS NUMERIC),
    TRIM(r.supplier_id),
    -- Impute NULL lead times with category average
    COALESCE(
        CAST(NULLIF(r.lead_time_days, '') AS INTEGER),
        clt.avg_lead_time,
        7   -- global fallback if entire category is NULL (shouldn't happen but defensive)
    ) AS lead_time_days,
    CAST(NULLIF(r.reorder_point, '') AS INTEGER),
    -- Normalize active flag: Y/YES/y/1/true → TRUE, everything else → FALSE
    CASE
        WHEN UPPER(TRIM(r.active)) IN ('Y', 'YES', '1', 'TRUE', 'T') THEN TRUE
        ELSE FALSE
    END AS active,
    CAST(NULLIF(r.last_updated, '') AS DATE)
FROM (
    SELECT *,
        -- THE KEY TRANSFORMATION: strip all separators, uppercase
        -- 'SKU-001' → 'SKU001', 'sku_001' → 'SKU001', 'SKU 001' → 'SKU001'
        UPPER(
            REPLACE(
                REPLACE(
                    REPLACE(TRIM(sku_id), '-', ''),  -- remove hyphens
                '-', ''),                             -- belt-and-suspenders (nested)
            ' ', '')                                  -- remove spaces
        ) AS cleaned_sku_id
    FROM (
        SELECT *,
            REPLACE(TRIM(sku_id), '_', '') AS sku_id  -- remove underscores
        FROM dim_sku_raw
    ) pre_clean
) r
LEFT JOIN category_lead_time_avg clt
    ON UPPER(TRIM(r.category)) = clt.category
WHERE r.cleaned_sku_id IS NOT NULL
  AND r.cleaned_sku_id != ''
ORDER BY cleaned_sku_id, r.last_updated DESC;  -- DISTINCT ON keeps most recent row per SKU

-- Verify fix:
-- SELECT COUNT(*) as total_skus,
--        SUM(CASE WHEN lead_time_days IS NULL THEN 1 ELSE 0 END) as still_null_lead_time
-- FROM dim_sku;
-- Expected: 200 rows (or fewer if duplicates after standardization), 0 NULL lead times


-- =============================================================================
-- FIX 3: Load service policy (clean table — just type-cast)
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
    UPPER(TRIM(customer_segment)),
    CAST(target_fill_rate AS NUMERIC),
    CAST(z_score AS NUMERIC),
    CAST(penalty_stockout_per_unit AS NUMERIC),
    CAST(priority_rank AS INTEGER),
    CAST(NULLIF(max_backorder_days, '') AS INTEGER),
    CAST(NULLIF(expedite_threshold, '') AS INTEGER),
    CAST(NULLIF(review_period_days, '') AS INTEGER),
    CAST(NULLIF(holding_cost_rate, '') AS NUMERIC),
    CAST(NULLIF(min_order_qty, '') AS INTEGER)
FROM dim_service_policy_raw;

-- Verify: z_score values should be 2.33, 1.65, 1.28 — from standard normal table
-- SELECT customer_segment, z_score, target_fill_rate FROM dim_service_policy ORDER BY priority_rank;


-- =============================================================================
-- FIX 4: Load lane costs (type-cast only, no injected issues)
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
SELECT
    TRIM(lane_id),
    TRIM(from_location),
    TRIM(to_location),
    UPPER(TRIM(transport_mode)),
    CAST(NULLIF(distance_miles, '') AS NUMERIC),
    CAST(transit_days_mean AS NUMERIC),
    CAST(NULLIF(transit_days_std, '') AS NUMERIC),
    CAST(cost_per_unit AS NUMERIC),
    CAST(NULLIF(cost_per_lb, '') AS NUMERIC)
FROM dim_lane_cost_raw
WHERE from_location IN (SELECT location_id FROM dim_location)
  AND to_location IN (SELECT location_id FROM dim_location);


-- =============================================================================
-- FIX 5: Deduplicate sales orders
--
-- WHY this deduplication approach (keep MAX ctid, not MIN):
--   The duplicate is a re-transmission of the same order from ERP.
--   The LATER record (higher ctid = loaded later in the file) is the one
--   the system re-sent — it's usually identical, but if any field was corrected
--   in the re-transmission, we want the correction, not the original error.
--   MAX(ctid) = most recently ingested = most likely to be the corrected version.
--
-- WHY we log the count before deleting:
--   Audit trail. In a real ERP environment, duplicate orders could indicate
--   a billing problem, a system bug, or fraud. You want to know the count
--   even after you've cleaned it.
-- =============================================================================

-- Log duplicate count before removal (this becomes your data quality report number)
DO $$
DECLARE
    dup_count INTEGER;
BEGIN
    SELECT COUNT(*) - COUNT(DISTINCT order_id)
    INTO dup_count
    FROM fact_sales_orders_raw;
    RAISE NOTICE 'Duplicate orders found and removed: %', dup_count;
END $$;

-- Insert deduplicated orders into clean table
-- DISTINCT ON (order_id) keeps the last-ingested row per order_id
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
SELECT DISTINCT ON (order_id)
    TRIM(order_id),
    CAST(order_date AS DATE),
    -- Apply same SKU standardization as dim_sku so FKs resolve
    UPPER(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''), 'SKU', 'SKU')) AS sku_id,
    TRIM(customer_id),
    NULLIF(TRIM(ship_to_location), ''),
    UPPER(TRIM(customer_segment)),
    CAST(qty_ordered AS INTEGER),
    CAST(price_per_unit AS NUMERIC),
    TRIM(channel),
    CASE WHEN UPPER(TRIM(priority_flag)) IN ('Y', 'YES', '1', 'TRUE') THEN TRUE ELSE FALSE END
FROM fact_sales_orders_raw
WHERE order_id IS NOT NULL
  AND order_id != ''
  -- Only keep orders where sku_id resolves to a known SKU after standardization
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''))
      IN (SELECT sku_id FROM dim_sku)
ORDER BY order_id, ctid DESC;  -- keep MAX ctid (most recently loaded row)

-- Verify: should be 490 rows (500 - 10 duplicates)
-- SELECT COUNT(*) FROM fact_sales_orders;
-- Expected: 490


-- =============================================================================
-- FIX 6: Cap freight outliers + impute NULL freight costs in shipments
--
-- WHY cap at P99 instead of dropping outliers:
--   Dropping means we lose the shipment record entirely — then qty_shipped
--   doesn't balance against inventory. We keep the row, we just correct the cost.
--   P99 is the standard Winsorization threshold: preserves the data distribution
--   while removing the influence of data entry errors (297072 instead of 2970).
--
-- WHY impute NULLs with mode-average (transport_mode average, not global):
--   Freight costs vary enormously by mode: AIR >> TRUCK >> RAIL
--   A NULL AIR shipment imputed with the TRUCK average would dramatically
--   understate cost-to-serve for premium customers who use air freight.
--   Mode-level imputation is the minimum granularity that makes business sense.
--
-- ORDER OF OPERATIONS: cap outliers FIRST, then compute mode averages for imputation.
--   Why: if you compute mode averages including the $297K outlier, your average
--   becomes inflated and the imputed values are wrong too. Cap first, average second.
-- =============================================================================

-- Step 1: Find P99 threshold for freight cost (per transport mode)
-- Step 2: Cap outliers at P99
-- Step 3: Impute NULLs with mode average (computed after capping)
-- All done in one INSERT using CTEs

WITH
-- Parse freight costs — exclude NULL and non-numeric values
valid_freight AS (
    SELECT
        transport_mode,
        CAST(freight_cost_usd AS NUMERIC) AS freight_cost
    FROM fact_shipments_raw
    WHERE freight_cost_usd IS NOT NULL
      AND freight_cost_usd != ''
      AND freight_cost_usd ~ '^[0-9]+(\.[0-9]+)?$'
),
-- Compute P99 per transport mode (our outlier cap)
freight_p99 AS (
    SELECT
        transport_mode,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY freight_cost) AS p99_threshold
    FROM valid_freight
    GROUP BY transport_mode
),
-- Compute mode averages AFTER capping (cap first, then average — order matters)
freight_avg_post_cap AS (
    SELECT
        v.transport_mode,
        AVG(LEAST(v.freight_cost, p.p99_threshold)) AS avg_freight_capped
    FROM valid_freight v
    JOIN freight_p99 p ON v.transport_mode = p.transport_mode
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
SELECT
    TRIM(r.shipment_id),
    NULLIF(TRIM(r.order_id), ''),
    CAST(NULLIF(r.ship_date, '') AS DATE),
    CAST(NULLIF(r.delivery_date, '') AS DATE),
    NULLIF(TRIM(r.from_location), ''),
    NULLIF(TRIM(r.to_location), ''),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(r.sku_id), '-', ''), '_', ''), ' ', '')) AS sku_id,
    CAST(NULLIF(r.qty_shipped, '') AS INTEGER),
    UPPER(TRIM(r.transport_mode)),
    -- The freight cost logic: NULL → impute, outlier → cap, normal → keep
    CASE
        WHEN r.freight_cost_usd IS NULL OR r.freight_cost_usd = ''
            -- NULL: impute with mode average (computed after capping)
            THEN avg.avg_freight_capped
        WHEN CAST(r.freight_cost_usd AS NUMERIC) > p99.p99_threshold
            -- Outlier: cap at P99 threshold
            THEN p99.p99_threshold
        ELSE
            -- Normal value: cast and keep
            CAST(r.freight_cost_usd AS NUMERIC)
    END AS freight_cost_usd,
    TRIM(r.carrier),
    CASE WHEN UPPER(TRIM(r.on_time_flag)) IN ('Y', 'YES', '1', 'TRUE') THEN TRUE ELSE FALSE END,
    CASE WHEN UPPER(TRIM(r.damage_flag)) IN ('Y', 'YES', '1', 'TRUE') THEN TRUE ELSE FALSE END
FROM fact_shipments_raw r
LEFT JOIN freight_p99 p99
    ON UPPER(TRIM(r.transport_mode)) = p99.transport_mode
LEFT JOIN freight_avg_post_cap avg
    ON UPPER(TRIM(r.transport_mode)) = avg.transport_mode
WHERE r.shipment_id IS NOT NULL
  AND r.shipment_id != '';

-- Verify: 400 rows, no NULLs in freight_cost_usd, no values above P99
-- SELECT
--   COUNT(*) as total,
--   SUM(CASE WHEN freight_cost_usd IS NULL THEN 1 ELSE 0 END) as null_freight,
--   MAX(freight_cost_usd) as max_freight,
--   AVG(freight_cost_usd) as avg_freight
-- FROM fact_shipments;
-- Expected: 400 rows, 0 NULLs, max_freight << 297072


-- =============================================================================
-- FIX 7: Floor negative on_hand_qty to 0 in inventory snapshots
--
-- WHY floor to 0, not NULL or drop the row:
--   NULL would exclude the location-SKU-date combination from the mart queries
--   entirely — your safety stock calculation would show no data for that period.
--   Dropping the row has the same problem plus loses the snapshot date record.
--
--   0 is the correct business value: the WMS sync bug produced a negative number
--   but the physical inventory cannot be negative. The floor to 0 reflects reality.
--   GREATEST(on_hand_qty, 0) is idiomatic SQL for this exact pattern.
--
-- WHY GREATEST() instead of a CASE WHEN:
--   Same result, but GREATEST() is the standard SQL idiom for flooring.
--   It's more readable to any SQL developer who looks at this code later.
--   CASE WHEN on_hand_qty < 0 THEN 0 ELSE on_hand_qty END works but reads as wordy.
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
SELECT
    CAST(snapshot_date AS DATE),
    TRIM(location_id),
    UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', '')) AS sku_id,
    -- THE KEY FIX: floor negative inventory to 0
    GREATEST(CAST(on_hand_qty AS INTEGER), 0) AS on_hand_qty,
    GREATEST(COALESCE(CAST(NULLIF(on_order_qty, '') AS INTEGER), 0), 0),
    GREATEST(COALESCE(CAST(NULLIF(backorder_qty, '') AS INTEGER), 0), 0),
    CAST(NULLIF(safety_stock_qty, '') AS INTEGER),
    CAST(NULLIF(reorder_point_qty, '') AS INTEGER),
    CAST(NULLIF(last_receipt_date, '') AS DATE),
    CAST(NULLIF(last_issue_date, '') AS DATE)
FROM fact_inventory_snapshot_raw
WHERE snapshot_date IS NOT NULL
  AND location_id IN (SELECT location_id FROM dim_location)
  AND UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''))
      IN (SELECT sku_id FROM dim_sku);

-- Verify: 300 rows, 0 negative on_hand values
-- SELECT
--   COUNT(*) as total,
--   SUM(CASE WHEN on_hand_qty < 0 THEN 1 ELSE 0 END) as still_negative,
--   MIN(on_hand_qty) as min_inventory
-- FROM fact_inventory_snapshot;
-- Expected: 300 rows, 0 negative, min_inventory = 0


-- =============================================================================
-- FINAL: Data Quality Audit Report
-- Run this block after cleaning to document what was fixed — use this in your portfolio
-- =============================================================================
SELECT
    'SKU standardization'       AS fix_applied,
    (SELECT COUNT(DISTINCT
        UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''))
     FROM dim_sku_raw)           AS records_after_clean,
    200                          AS records_before_clean,
    'Mixed formats unified (SKU-001/SKU001/sku_001 → SKU001)' AS method

UNION ALL SELECT
    'Lead time imputation',
    (SELECT COUNT(*) FROM dim_sku WHERE lead_time_days IS NOT NULL),
    (SELECT COUNT(*) FROM dim_sku_raw WHERE lead_time_days IS NOT NULL AND lead_time_days != ''),
    'Category-average imputation (Electronics avg ≠ Bulk avg)'

UNION ALL SELECT
    'Order deduplication',
    (SELECT COUNT(*) FROM fact_sales_orders),
    500,
    'DISTINCT ON order_id, keep MAX ctid (most recent ERP transmission)'

UNION ALL SELECT
    'Freight outlier capping',
    (SELECT COUNT(*) FROM fact_shipments WHERE freight_cost_usd > 0),
    (SELECT COUNT(*) FROM fact_shipments_raw WHERE freight_cost_usd IS NOT NULL AND freight_cost_usd != ''),
    'Winsorization at P99 per transport mode'

UNION ALL SELECT
    'Freight NULL imputation',
    (SELECT COUNT(*) FROM fact_shipments WHERE freight_cost_usd IS NOT NULL),
    (SELECT COUNT(*) FROM fact_shipments_raw) - 30,
    'Mode-level average (post-capping) — AIR/TRUCK/RAIL imputed separately'

UNION ALL SELECT
    'Negative inventory correction',
    (SELECT COUNT(*) FROM fact_inventory_snapshot WHERE on_hand_qty = 0),
    9,
    'GREATEST(on_hand_qty, 0) — WMS sync bug, physical inventory cannot be negative';
