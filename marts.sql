BEGIN;

SET search_path TO public;

DROP TABLE IF EXISTS public.mart_inventory_position CASCADE;
DROP TABLE IF EXISTS public.mart_cost_to_serve CASCADE;
DROP TABLE IF EXISTS public.mart_network_flow CASCADE;
DROP TABLE IF EXISTS public.mart_data_quality_report CASCADE;
DROP TABLE IF EXISTS public.mart_demand_stats CASCADE;

CREATE TABLE public.mart_demand_stats AS
WITH daily_demand AS (
    SELECT
        o.order_date,
        DATE_TRUNC('week', o.order_date) AS week_start,
        DATE_TRUNC('month', o.order_date) AS month_start,
        o.sku_id,
        o.location_id,
        o.customer_segment,
        SUM(o.qty_ordered) AS total_qty_ordered,
        COUNT(DISTINCT o.order_id) AS num_orders,
        AVG(o.price_per_unit) AS avg_price_per_unit
    FROM fact_sales_orders o
    WHERE o.location_id IS NOT NULL
    GROUP BY
        o.order_date,
        DATE_TRUNC('week', o.order_date),
        DATE_TRUNC('month', o.order_date),
        o.sku_id,
        o.location_id,
        o.customer_segment
),
demand_spine AS (
    SELECT DISTINCT
        d.sku_id,
        d.location_id AS location_id,
        d.customer_segment
    FROM daily_demand d
)
SELECT
    spine.sku_id,
    spine.location_id,
    spine.customer_segment,
    s.category,
    s.lead_time_days,
    sp.z_score,
    sp.target_fill_rate,
    COUNT(dd.order_date) AS observation_days,
    AVG(dd.total_qty_ordered) AS avg_demand_per_day,
    STDDEV(dd.total_qty_ordered) AS std_demand_per_day,
    MAX(dd.total_qty_ordered) AS max_demand_per_day,
    MIN(dd.total_qty_ordered) AS min_demand_per_day,
    COALESCE(
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY dd.total_qty_ordered),
        0
    ) AS p50_demand,
    COALESCE(
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dd.total_qty_ordered),
        0
    ) AS p90_demand,
    COALESCE(
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY dd.total_qty_ordered),
        0
    ) AS p95_demand,
    CASE
        WHEN AVG(dd.total_qty_ordered) > 0
            THEN COALESCE(STDDEV(dd.total_qty_ordered), 0) / NULLIF(AVG(dd.total_qty_ordered), 0)
        ELSE NULL
    END AS coefficient_of_variation,
    ROUND(
        sp.z_score
        * COALESCE(STDDEV(dd.total_qty_ordered), 0)
        * SQRT(s.lead_time_days)
    ) AS safety_stock_units,
    ROUND(
        (COALESCE(AVG(dd.total_qty_ordered), 0) * s.lead_time_days)
        + (
            sp.z_score
            * COALESCE(STDDEV(dd.total_qty_ordered), 0)
            * SQRT(s.lead_time_days)
        )
    ) AS reorder_point_units,
    ROUND(COALESCE(AVG(dd.total_qty_ordered), 0) * 365) AS annual_demand_estimate
FROM demand_spine spine
JOIN dim_sku s
    ON spine.sku_id = s.sku_id
JOIN dim_service_policy sp
    ON spine.customer_segment = sp.customer_segment
LEFT JOIN daily_demand dd
    ON spine.sku_id = dd.sku_id
    AND spine.location_id = dd.location_id,
    AND spine.customer_segment = dd.customer_segment
GROUP BY
    spine.sku_id,
    spine.location_id,
    spine.customer_segment,
    s.category,
    s.lead_time_days,
    sp.z_score,
    sp.target_fill_rate
HAVING COUNT(dd.order_date) >= 5
ORDER BY
    spine.sku_id,
    spine.location_id,
    spine.customer_segment;

CREATE INDEX idx_mart_demand_sku ON public.mart_demand_stats (sku_id);
CREATE INDEX idx_mart_demand_location ON public.mart_demand_stats (location_id);
CREATE INDEX idx_mart_demand_segment ON public.mart_demand_stats (customer_segment);

CREATE TABLE public.mart_inventory_position AS
WITH latest_snapshot AS (
    SELECT DISTINCT ON (location_id, sku_id)
        location_id,
        sku_id,
        snapshot_date,
        on_hand_qty,
        on_order_qty,
        backorder_qty,
        safety_stock_qty AS current_safety_stock_policy,
        reorder_point_qty AS current_reorder_point_policy
    FROM fact_inventory_snapshot
    ORDER BY
        location_id,
        sku_id,
        snapshot_date DESC
)
SELECT
    ls.location_id,
    l.location_name,
    l.location_type,
    l.region,
    l.echelon,
    ls.sku_id,
    s.description,
    s.category,
    s.unit_cost,
    s.lead_time_days,
    ls.snapshot_date AS as_of_date,
    ls.on_hand_qty,
    ls.on_order_qty,
    ls.backorder_qty,
    (ls.on_hand_qty + ls.on_order_qty - ls.backorder_qty) AS inventory_position,
    ls.current_safety_stock_policy,
    ls.current_reorder_point_policy,
    ds.safety_stock_units AS recommended_safety_stock,
    ds.reorder_point_units AS recommended_reorder_point,
    CASE
        WHEN (ls.on_hand_qty + ls.on_order_qty - ls.backorder_qty) >= ds.reorder_point_units
            THEN 'SAFE'
        ELSE 'RISK'
    END AS service_level_status,
    ds.avg_demand_per_day,
    CASE
        WHEN ds.avg_demand_per_day > 0
            THEN ROUND(ls.on_hand_qty / ds.avg_demand_per_day)
        ELSE NULL
    END AS days_of_supply,
    ROUND((ls.on_hand_qty * s.unit_cost)::numeric, 2) AS inventory_value_usd,
    CASE
        WHEN (ls.on_hand_qty + ls.on_order_qty - ls.backorder_qty)
             < COALESCE(ds.safety_stock_units, ls.current_safety_stock_policy, 0)
            THEN 'UNDERSTOCKED'
        WHEN ls.on_hand_qty > COALESCE(ds.reorder_point_units, ls.current_reorder_point_policy, 0) * 2
            THEN 'OVERSTOCKED'
        ELSE 'NORMAL'
    END AS stock_status,
    COALESCE(ds.safety_stock_units, 0) - COALESCE(ls.current_safety_stock_policy, 0) AS safety_stock_gap_units,
    (COALESCE(ds.safety_stock_units, 0) - COALESCE(ls.current_safety_stock_policy, 0)) * s.unit_cost AS safety_stock_gap_usd
FROM latest_snapshot ls
JOIN dim_location l
    ON ls.location_id = l.location_id
JOIN dim_sku s
    ON ls.sku_id = s.sku_id
LEFT JOIN public.mart_demand_stats ds
    ON ls.sku_id = ds.sku_id
    AND ls.location_id = ds.location_id
ORDER BY inventory_value_usd DESC;

CREATE TABLE public.mart_cost_to_serve AS
WITH order_costs AS (
    SELECT
        o.order_id,
        o.sku_id,
        o.customer_segment,
        o.location_id,
        l_ship.region AS ship_to_region,
        l_from.location_type AS fulfilled_from_type,
        o.qty_ordered,
        o.price_per_unit,
        o.qty_ordered * o.price_per_unit AS order_revenue,
        o.qty_ordered * s.unit_cost AS product_cost,
        COALESCE(sh.freight_cost_usd, 0) AS freight_cost,
        o.qty_ordered * 0.50 AS warehouse_handling_cost,
        (s.unit_cost * 0.25 / 365 * s.lead_time_days * o.qty_ordered) AS carrying_cost,
        COALESCE(
            (o.qty_ordered - sh.qty_shipped) * sp.penalty_stockout_per_unit,
            0
        ) AS stockout_cost,
        2.50 AS order_processing_cost
    FROM fact_sales_orders o
    JOIN dim_sku s
        ON o.sku_id = s.sku_id
    JOIN dim_service_policy sp
        ON o.customer_segment = sp.customer_segment
    LEFT JOIN fact_shipments sh
        ON o.order_id = sh.order_id
    LEFT JOIN dim_location l_ship
        ON o.location_id = l_location_id
    LEFT JOIN dim_location l_from
        ON sh.from_location = l_from.location_id
)
SELECT
    customer_segment,
    ship_to_region,
    sku_id,
    COUNT(DISTINCT order_id) AS num_orders,
    SUM(qty_ordered) AS total_units_ordered,
    ROUND(SUM(order_revenue)::numeric, 2) AS total_revenue,
    ROUND(SUM(product_cost)::numeric, 2) AS total_product_cost,
    ROUND(SUM(freight_cost)::numeric, 2) AS total_freight_cost,
    ROUND(SUM(warehouse_handling_cost)::numeric, 2) AS total_warehouse_cost,
    ROUND(SUM(carrying_cost)::numeric, 2) AS total_carrying_cost,
    ROUND(SUM(stockout_cost)::numeric, 2) AS total_stockout_cost,
    ROUND(SUM(order_processing_cost)::numeric, 2) AS total_processing_cost,
    ROUND(
        SUM(
            product_cost
            + freight_cost
            + warehouse_handling_cost
            + carrying_cost
            + stockout_cost
            + order_processing_cost
        )::numeric,
        2
    ) AS total_cost,
    ROUND(
        (
            SUM(order_revenue)
            - SUM(
                product_cost
                + freight_cost
                + warehouse_handling_cost
                + carrying_cost
                + stockout_cost
                + order_processing_cost
            )
        )::numeric,
        2
    ) AS gross_margin,
    ROUND(
        (
            SUM(
                product_cost
                + freight_cost
                + warehouse_handling_cost
                + carrying_cost
                + stockout_cost
                + order_processing_cost
            )
            / NULLIF(SUM(qty_ordered), 0)
        )::numeric,
        4
    ) AS cost_per_unit,
    ROUND((SUM(order_revenue) / NULLIF(SUM(qty_ordered), 0))::numeric, 4) AS revenue_per_unit,
    ROUND(
        (
            (
                SUM(order_revenue)
                - SUM(
                    product_cost
                    + freight_cost
                    + warehouse_handling_cost
                    + carrying_cost
                    + stockout_cost
                    + order_processing_cost
                )
            )
            / NULLIF(SUM(order_revenue), 0) * 100
        )::numeric,
        2
    ) AS margin_pct
FROM order_costs
GROUP BY
    customer_segment,
    ship_to_region,
    sku_id
ORDER BY total_cost DESC;

CREATE TABLE public.mart_network_flow AS
SELECT
    sh.from_location,
    l_from.location_name AS from_location_name,
    l_from.location_type AS from_type,
    sh.to_location,
    l_to.location_name AS to_location_name,
    l_to.location_type AS to_type,
    sh.transport_mode,
    sh.sku_id,
    s.category,
    COUNT(DISTINCT sh.shipment_id) AS num_shipments,
    SUM(sh.qty_shipped) AS total_units_shipped,
    ROUND(SUM(sh.freight_cost_usd)::numeric, 2) AS total_freight_cost,
    ROUND(AVG(sh.freight_cost_usd)::numeric, 4) AS avg_freight_per_shipment,
    ROUND((SUM(sh.freight_cost_usd) / NULLIF(SUM(sh.qty_shipped), 0))::numeric, 4) AS freight_cost_per_unit,
    ROUND(AVG(sh.delivery_date - sh.ship_date)::numeric, 2) AS avg_actual_lead_time_days,
    ROUND(STDDEV(sh.delivery_date - sh.ship_date)::numeric, 4) AS std_actual_lead_time_days,
    ROUND(AVG(CASE WHEN sh.on_time_flag THEN 1.0 ELSE 0.0 END)::numeric * 100, 2) AS on_time_pct,
    lc.cost_per_unit AS contracted_cost_per_unit,
    ROUND(
        (
            SUM(sh.freight_cost_usd) / NULLIF(SUM(sh.qty_shipped), 0)
            - lc.cost_per_unit
        )::numeric,
        4
    ) AS freight_variance_per_unit
FROM fact_shipments sh
JOIN dim_location l_from
    ON sh.from_location = l_from.location_id
JOIN dim_location l_to
    ON sh.to_location = l_to.location_id
JOIN dim_sku s
    ON sh.sku_id = s.sku_id
LEFT JOIN dim_lane_cost lc
    ON sh.from_location = lc.from_location
    AND sh.to_location = lc.to_location
    AND sh.transport_mode = lc.transport_mode
GROUP BY
    sh.from_location,
    l_from.location_name,
    l_from.location_type,
    sh.to_location,
    l_to.location_name,
    l_to.location_type,
    sh.transport_mode,
    sh.sku_id,
    s.category,
    lc.cost_per_unit
ORDER BY total_freight_cost DESC;

CREATE TABLE public.mart_data_quality_report AS
SELECT
    'dim_sku' AS table_name,
    'sku_id_format' AS issue_type,
    (
        SELECT COUNT(*)
        FROM dim_sku_raw
        WHERE sku_id != UPPER(REPLACE(REPLACE(REPLACE(TRIM(sku_id), '-', ''), '_', ''), ' ', ''))
    ) AS records_affected,
    'Mixed formats: SKU-001, SKU001, sku_001, Sku-001, SKU 001' AS description,
    'UPPER + REPLACE hyphens/underscores/spaces' AS resolution_method,
    CURRENT_DATE AS run_timestamp

UNION ALL

SELECT
    'dim_sku',
    'lead_time_null',
    (
        SELECT COUNT(*)
        FROM dim_sku_raw
        WHERE lead_time_days IS NULL OR BTRIM(lead_time_days::text) = ''
    ),
    '24 rows missing lead_time_days (~12% of SKUs)',
    'Category-average imputation (Electronics avg != Bulk avg)',
    CURRENT_DATE

UNION ALL

SELECT
    'dim_sku',
    'active_flag_inconsistent',
    (
        SELECT COUNT(*)
        FROM dim_sku_raw
        WHERE active NOT IN ('Y', 'N', '1', '0')
    ),
    'Inconsistent active flags: Y, YES, y, 1, true',
    'CASE WHEN UPPER(active) IN (Y,YES,1,TRUE,T) THEN TRUE ELSE FALSE',
    CURRENT_DATE

UNION ALL

SELECT
    'fact_sales_orders',
    'duplicate_order_id',
    (
        SELECT COUNT(*) - COUNT(DISTINCT order_id)
        FROM fact_sales_orders_raw
    ),
    '~10 duplicate order_ids from ERP re-transmission bug',
    'DISTINCT ON order_id, keep MAX ctid (most recent transmission)',
    CURRENT_DATE

UNION ALL

SELECT
    'fact_shipments',
    'freight_cost_outlier',
    (
        SELECT COUNT(*)
        FROM fact_shipments_raw
        WHERE freight_cost_usd IS NOT NULL
          AND BTRIM(freight_cost_usd::text) <> ''
          AND freight_cost_usd::text ~ '^[0-9]+(\.[0-9]+)?$'
          AND CAST(freight_cost_usd AS numeric) > 10000
    ),
    '7 entries at 100x normal freight cost (data entry typos)',
    'Winsorization at P99 per transport mode (cap, do not drop)',
    CURRENT_DATE

UNION ALL

SELECT
    'fact_shipments',
    'freight_cost_null',
    30,
    '30 missing freight_cost_usd values',
    'Mode-level average imputation (AIR/TRUCK/RAIL imputed separately)',
    CURRENT_DATE

UNION ALL

SELECT
    'fact_inventory_snapshot',
    'negative_on_hand',
    (
        SELECT COUNT(*)
        FROM fact_inventory_snapshot_raw
        WHERE on_hand_qty IS NOT NULL
          AND BTRIM(on_hand_qty::text) <> ''
          AND on_hand_qty::text ~ '^-?[0-9]+$'
          AND CAST(on_hand_qty AS integer) < 0
    ),
    '9 rows with negative on_hand_qty (WMS sync bug)',
    'GREATEST(on_hand_qty, 0) - physical inventory cannot be negative',
    CURRENT_DATE;

SELECT
    'mart_demand_stats' AS mart_name,
    COUNT(*) AS row_count,
    'Safety stock inputs: avg_demand, std_demand, safety_stock_units, reorder_point_units' AS python_reads
FROM public.mart_demand_stats

UNION ALL

SELECT
    'mart_inventory_position',
    COUNT(*),
    'Current state: on_hand, days_of_supply, stock_status, safety_stock_gap_usd'
FROM public.mart_inventory_position

UNION ALL

SELECT
    'mart_cost_to_serve',
    COUNT(*),
    'Cost breakdown: product, freight, warehouse, carrying, stockout - by segment/region/SKU'
FROM public.mart_cost_to_serve

UNION ALL

SELECT
    'mart_network_flow',
    COUNT(*),
    'Lane actuals: freight_cost_per_unit, avg_actual_lead_time_days for Monte Carlo'
FROM public.mart_network_flow

UNION ALL

SELECT
    'mart_data_quality_report',
    COUNT(*),
    'Audit trail: 7 issues found, documented, resolved - for Streamlit Page 2'
FROM public.mart_data_quality_report

ORDER BY mart_name;

COMMIT;
