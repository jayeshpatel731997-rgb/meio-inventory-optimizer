-- ============================================================
-- MEIO OPTIMIZER — data_quality_report.sql
-- Run AFTER: ingest.sql (on raw tables, BEFORE cleaning)
-- Shows what issues exist — the "before" picture
-- This is what you show recruiters to prove ERP data competence
-- ============================================================

-- ============================================================
-- REPORT 1: Summary — Issues found per table
-- ============================================================

SELECT
    'dim_sku_raw'                                                               AS table_name,
    'PLM'                                                                       AS source_system,
    COUNT(*)                                                                    AS total_rows,
    COUNT(DISTINCT sku_id)                                                      AS unique_keys,
    COUNT(*) - COUNT(DISTINCT sku_id)                                          AS duplicate_rows,
    SUM(CASE WHEN lead_time_days IS NULL THEN 1 ELSE 0 END)                    AS missing_lead_time,
    SUM(CASE WHEN unit_cost IS NULL OR unit_cost <= 0 THEN 1 ELSE 0 END)       AS invalid_cost,
    SUM(CASE WHEN sku_id != UPPER(REPLACE(REPLACE(sku_id,'-',''),'_',''))
             THEN 1 ELSE 0 END)                                                 AS inconsistent_sku_format,
    ROUND(
        COUNT(DISTINCT sku_id) * 100.0 / COUNT(*), 1
    )                                                                           AS quality_score_pct
FROM dim_sku_raw

UNION ALL

SELECT
    'dim_location_raw'                                                          AS table_name,
    'WMS'                                                                       AS source_system,
    COUNT(*)                                                                    AS total_rows,
    COUNT(DISTINCT location_id)                                                 AS unique_keys,
    COUNT(*) - COUNT(DISTINCT location_id)                                     AS duplicate_rows,
    SUM(CASE WHEN capacity_units IS NULL THEN 1 ELSE 0 END)                    AS missing_capacity,
    0                                                                           AS invalid_cost,
    SUM(CASE WHEN location_id LIKE 'DC-%' THEN 1 ELSE 0 END)                  AS old_naming_convention,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN capacity_units IS NULL THEN 1 ELSE 0 END))
        * 100.0 / COUNT(*), 1
    )                                                                           AS quality_score_pct
FROM dim_location_raw

UNION ALL

SELECT
    'fact_sales_orders_raw'                                                     AS table_name,
    'ERP'                                                                       AS source_system,
    COUNT(*)                                                                    AS total_rows,
    COUNT(DISTINCT order_id)                                                    AS unique_keys,
    COUNT(*) - COUNT(DISTINCT order_id)                                        AS duplicate_rows,
    SUM(CASE WHEN customer_segment IS NULL THEN 1 ELSE 0 END)                  AS missing_segment,
    SUM(CASE WHEN qty_ordered <= 0 THEN 1 ELSE 0 END)                         AS invalid_qty,
    SUM(CASE WHEN customer_segment NOT IN ('PREMIUM','STANDARD','BUDGET')
             AND customer_segment IS NOT NULL THEN 1 ELSE 0 END)               AS nonstandard_segment,
    ROUND(
        COUNT(DISTINCT order_id) * 100.0 / COUNT(*), 1
    )                                                                           AS quality_score_pct
FROM fact_sales_orders_raw

UNION ALL

SELECT
    'fact_shipments_raw'                                                        AS table_name,
    'TMS'                                                                       AS source_system,
    COUNT(*)                                                                    AS total_rows,
    COUNT(DISTINCT shipment_id)                                                 AS unique_keys,
    COUNT(*) - COUNT(DISTINCT shipment_id)                                     AS duplicate_rows,
    SUM(CASE WHEN freight_cost IS NULL THEN 1 ELSE 0 END)                     AS missing_freight,
    SUM(CASE WHEN delivery_date < ship_date THEN 1 ELSE 0 END)                AS invalid_dates,
    SUM(CASE WHEN freight_cost > (
        SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY freight_cost)
        FROM fact_shipments_raw WHERE freight_cost IS NOT NULL
    ) THEN 1 ELSE 0 END)                                                       AS outlier_freight,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN freight_cost IS NULL THEN 1 ELSE 0 END))
        * 100.0 / COUNT(*), 1
    )                                                                           AS quality_score_pct
FROM fact_shipments_raw

UNION ALL

SELECT
    'fact_inventory_snapshot_raw'                                               AS table_name,
    'WMS'                                                                       AS source_system,
    COUNT(*)                                                                    AS total_rows,
    COUNT(DISTINCT (snapshot_date::TEXT || location_id || sku_id))             AS unique_keys,
    COUNT(*) - COUNT(DISTINCT (snapshot_date::TEXT || location_id || sku_id)) AS duplicate_rows,
    SUM(CASE WHEN on_hand_qty < 0 THEN 1 ELSE 0 END)                          AS negative_inventory,
    0                                                                           AS invalid_cost,
    0                                                                           AS other_issues,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN on_hand_qty < 0 THEN 1 ELSE 0 END))
        * 100.0 / COUNT(*), 1
    )                                                                           AS quality_score_pct
FROM fact_inventory_snapshot_raw;

-- ============================================================
-- REPORT 2: SKU Code Inconsistency Detail
-- Shows exactly what formats exist before standardization
-- ============================================================

SELECT
    CASE
        WHEN sku_id ~ '^SKU-[0-9]+'    THEN 'Format: SKU-001'
        WHEN sku_id ~ '^SKU[0-9]+'     THEN 'Format: SKU001'
        WHEN sku_id ~ '^sku_[0-9]+'    THEN 'Format: sku_001'
        WHEN sku_id ~ '^[A-Z]{3}[0-9]+'THEN 'Format: ABC001'
        ELSE 'Other format'
    END                             AS format_type,
    COUNT(*)                        AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM dim_sku_raw
GROUP BY format_type
ORDER BY count DESC;

-- ============================================================
-- REPORT 3: Freight Cost Outlier Analysis
-- Shows the 100x typo problem before capping
-- ============================================================

WITH freight_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY freight_cost) AS p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY freight_cost) AS p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY freight_cost) AS p75,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY freight_cost) AS p99,
        MAX(freight_cost)                                           AS max_val,
        AVG(freight_cost)                                           AS mean_val
    FROM fact_shipments_raw
    WHERE freight_cost IS NOT NULL
)
SELECT
    ROUND(p25, 2)   AS p25_freight,
    ROUND(p50, 2)   AS p50_freight,
    ROUND(p75, 2)   AS p75_freight,
    ROUND(p99, 2)   AS p99_freight_cap,
    ROUND(max_val, 2) AS max_freight_before_cap,
    ROUND(mean_val, 2) AS mean_freight,
    ROUND(max_val / NULLIF(p99, 0), 1) AS max_to_p99_ratio   -- should show ~100x for typos
FROM freight_stats;

-- ============================================================
-- REPORT 4: Overall Data Quality Score Card
-- This is what you show in your Streamlit dashboard
-- ============================================================

WITH scores AS (
    SELECT 'dim_sku_raw'            AS tbl, COUNT(DISTINCT sku_id) * 100.0 / COUNT(*) AS score FROM dim_sku_raw
    UNION ALL
    SELECT 'dim_location_raw',      COUNT(DISTINCT location_id) * 100.0 / COUNT(*) FROM dim_location_raw
    UNION ALL
    SELECT 'fact_sales_orders_raw', COUNT(DISTINCT order_id) * 100.0 / COUNT(*) FROM fact_sales_orders_raw
    UNION ALL
    SELECT 'fact_shipments_raw',    COUNT(DISTINCT shipment_id) * 100.0 / COUNT(*) FROM fact_shipments_raw
)
SELECT
    tbl                         AS table_name,
    ROUND(score, 1)             AS quality_score_pct,
    CASE
        WHEN score >= 90 THEN '🟢 GOOD'
        WHEN score >= 75 THEN '🟡 NEEDS CLEANING'
        ELSE '🔴 POOR'
    END                         AS status
FROM scores
ORDER BY score ASC;
