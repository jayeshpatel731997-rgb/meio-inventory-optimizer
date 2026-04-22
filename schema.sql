-- =============================================================================
-- MEIO OPTIMIZER — schema.sql
-- Purpose: Define all raw staging tables + clean production tables
-- Pattern: raw_* tables accept dirty CSV data as-is (all TEXT)
--          Clean tables enforce types, constraints, foreign keys
-- Why two layers: Never destroy source data. Cleaning is a transform, not a delete.
-- =============================================================================

-- Drop in reverse dependency order (FKs first)
DROP TABLE IF EXISTS fact_shipments_raw CASCADE;
DROP TABLE IF EXISTS fact_sales_orders_raw CASCADE;
DROP TABLE IF EXISTS fact_inventory_snapshot_raw CASCADE;
DROP TABLE IF EXISTS dim_lane_cost_raw CASCADE;
DROP TABLE IF EXISTS dim_service_policy_raw CASCADE;
DROP TABLE IF EXISTS dim_sku_raw CASCADE;
DROP TABLE IF EXISTS dim_location_raw CASCADE;

DROP TABLE IF EXISTS fact_shipments CASCADE;
DROP TABLE IF EXISTS fact_sales_orders CASCADE;
DROP TABLE IF EXISTS fact_inventory_snapshot CASCADE;
DROP TABLE IF EXISTS dim_lane_cost CASCADE;
DROP TABLE IF EXISTS dim_service_policy CASCADE;
DROP TABLE IF EXISTS dim_sku CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;

-- =============================================================================
-- RAW STAGING TABLES (all TEXT — accepts anything from CSV)
-- =============================================================================

-- Raw SKU master — will have mixed formats: SKU-001, SKU001, sku_001, etc.
CREATE TABLE dim_sku_raw (
    sku_id              TEXT,
    description         TEXT,
    category            TEXT,
    subcategory         TEXT,
    unit_cost           TEXT,
    unit_volume_cuft    TEXT,
    unit_weight_lbs     TEXT,
    supplier_id         TEXT,
    lead_time_days      TEXT,   -- 24 rows intentionally NULL
    reorder_point       TEXT,
    active              TEXT,   -- mixed: Y, YES, y, 1, true
    last_updated        TEXT
);

-- Raw locations — clean anchor table, still load as TEXT for safety
CREATE TABLE dim_location_raw (
    location_id             TEXT,
    location_name           TEXT,
    location_type           TEXT,   -- DC, RDC, STORE
    region                  TEXT,
    echelon                 TEXT,
    capacity_units          TEXT,
    storage_cost_per_unit   TEXT,
    fixed_operating_cost    TEXT
);

-- Raw sales orders — contains ~10 duplicate order_ids
CREATE TABLE fact_sales_orders_raw (
    order_id            TEXT,
    order_date          TEXT,
    sku_id              TEXT,
    customer_id         TEXT,
    ship_to_location    TEXT,
    customer_segment    TEXT,   -- PREMIUM, STANDARD, BUDGET
    qty_ordered         TEXT,
    price_per_unit      TEXT,
    channel             TEXT,
    priority_flag       TEXT
);

-- Raw shipments — 30 missing freight_cost_usd, 7 at 100x normal value
CREATE TABLE fact_shipments_raw (
    shipment_id         TEXT,
    order_id            TEXT,
    ship_date           TEXT,
    delivery_date       TEXT,
    from_location       TEXT,
    to_location         TEXT,
    sku_id              TEXT,
    qty_shipped         TEXT,
    transport_mode      TEXT,   -- TRUCK, AIR, RAIL
    freight_cost_usd    TEXT,   -- NULLs and typos like 297072 instead of 2970
    carrier             TEXT,
    on_time_flag        TEXT,
    damage_flag         TEXT
);

-- Raw inventory snapshots — 9 rows with negative on_hand_qty (WMS sync bug)
CREATE TABLE fact_inventory_snapshot_raw (
    snapshot_date       TEXT,
    location_id         TEXT,
    sku_id              TEXT,
    on_hand_qty         TEXT,   -- negative values exist
    on_order_qty        TEXT,
    backorder_qty       TEXT,
    safety_stock_qty    TEXT,
    reorder_point_qty   TEXT,
    last_receipt_date   TEXT,
    last_issue_date     TEXT
);

-- Raw lane costs — DC→RDC and RDC→STORE shipping lanes
CREATE TABLE dim_lane_cost_raw (
    lane_id             TEXT,
    from_location       TEXT,
    to_location         TEXT,
    transport_mode      TEXT,
    distance_miles      TEXT,
    transit_days_mean   TEXT,
    transit_days_std    TEXT,
    cost_per_unit       TEXT,
    cost_per_lb         TEXT
);

-- Raw service policy — 3 rows: PREMIUM, STANDARD, BUDGET
CREATE TABLE dim_service_policy_raw (
    customer_segment        TEXT,
    target_fill_rate        TEXT,
    z_score                 TEXT,   -- 2.33, 1.65, 1.28 — from standard normal table
    penalty_stockout_per_unit TEXT,
    priority_rank           TEXT,
    max_backorder_days      TEXT,
    expedite_threshold      TEXT,
    review_period_days      TEXT,
    holding_cost_rate       TEXT,
    min_order_qty           TEXT
);


-- =============================================================================
-- CLEAN PRODUCTION TABLES (typed, constrained, FK-enforced)
-- =============================================================================

-- Clean SKU master — sku_id standardized to UPPER, no hyphens/underscores
CREATE TABLE dim_sku (
    sku_id              VARCHAR(50)     PRIMARY KEY,
    description         VARCHAR(200)    NOT NULL,
    category            VARCHAR(50)     NOT NULL,
    subcategory         VARCHAR(50),
    unit_cost           NUMERIC(10,2)   NOT NULL CHECK (unit_cost > 0),
    unit_volume_cuft    NUMERIC(10,4),
    unit_weight_lbs     NUMERIC(10,4),
    supplier_id         VARCHAR(50),
    lead_time_days      INTEGER         NOT NULL,   -- NULLs imputed from category avg
    reorder_point       INTEGER,
    active              BOOLEAN         NOT NULL DEFAULT TRUE,
    last_updated        DATE
);

-- Clean location master — network spine, every fact table joins here
CREATE TABLE dim_location (
    location_id             VARCHAR(50)     PRIMARY KEY,
    location_name           VARCHAR(100)    NOT NULL,
    location_type           VARCHAR(20)     NOT NULL CHECK (location_type IN ('DC', 'RDC', 'STORE')),
    region                  VARCHAR(50)     NOT NULL,
    echelon                 INTEGER         NOT NULL CHECK (echelon IN (1, 2, 3)),
    capacity_units          INTEGER,
    storage_cost_per_unit   NUMERIC(10,4),
    fixed_operating_cost    NUMERIC(12,2)
);

-- Clean sales orders — deduped, typed, FK-enforced
CREATE TABLE fact_sales_orders (
    order_id            VARCHAR(50)     PRIMARY KEY,
    order_date          DATE            NOT NULL,
    sku_id              VARCHAR(50)     NOT NULL REFERENCES dim_sku(sku_id),
    customer_id         VARCHAR(50),
    ship_to_location    VARCHAR(50)     REFERENCES dim_location(location_id),
    customer_segment    VARCHAR(20)     NOT NULL CHECK (customer_segment IN ('PREMIUM', 'STANDARD', 'BUDGET')),
    qty_ordered         INTEGER         NOT NULL CHECK (qty_ordered > 0),
    price_per_unit      NUMERIC(10,2)   NOT NULL CHECK (price_per_unit > 0),
    channel             VARCHAR(30),
    priority_flag       BOOLEAN         DEFAULT FALSE
);

-- Clean shipments — freight outliers capped, NULLs imputed
CREATE TABLE fact_shipments (
    shipment_id         VARCHAR(50)     PRIMARY KEY,
    order_id            VARCHAR(50)     REFERENCES fact_sales_orders(order_id),
    ship_date           DATE,
    delivery_date       DATE,
    from_location       VARCHAR(50)     REFERENCES dim_location(location_id),
    to_location         VARCHAR(50)     REFERENCES dim_location(location_id),
    sku_id              VARCHAR(50)     REFERENCES dim_sku(sku_id),
    qty_shipped         INTEGER         CHECK (qty_shipped >= 0),
    transport_mode      VARCHAR(20)     CHECK (transport_mode IN ('TRUCK', 'AIR', 'RAIL')),
    freight_cost_usd    NUMERIC(10,2)   CHECK (freight_cost_usd >= 0),
    carrier             VARCHAR(50),
    on_time_flag        BOOLEAN,
    damage_flag         BOOLEAN
);

-- Clean inventory snapshots — negative on_hand floored to 0
CREATE TABLE fact_inventory_snapshot (
    snapshot_date       DATE            NOT NULL,
    location_id         VARCHAR(50)     NOT NULL REFERENCES dim_location(location_id),
    sku_id              VARCHAR(50)     NOT NULL REFERENCES dim_sku(sku_id),
    on_hand_qty         INTEGER         NOT NULL CHECK (on_hand_qty >= 0),  -- enforced after GREATEST(x,0)
    on_order_qty        INTEGER         DEFAULT 0 CHECK (on_order_qty >= 0),
    backorder_qty       INTEGER         DEFAULT 0 CHECK (backorder_qty >= 0),
    safety_stock_qty    INTEGER         CHECK (safety_stock_qty >= 0),
    reorder_point_qty   INTEGER,
    last_receipt_date   DATE,
    last_issue_date     DATE,
    PRIMARY KEY (snapshot_date, location_id, sku_id)
);

-- Clean lane costs — freight engine for cost-to-serve
CREATE TABLE dim_lane_cost (
    lane_id             VARCHAR(50)     PRIMARY KEY,
    from_location       VARCHAR(50)     NOT NULL REFERENCES dim_location(location_id),
    to_location         VARCHAR(50)     NOT NULL REFERENCES dim_location(location_id),
    transport_mode      VARCHAR(20)     NOT NULL CHECK (transport_mode IN ('TRUCK', 'AIR', 'RAIL')),
    distance_miles      NUMERIC(8,2),
    transit_days_mean   NUMERIC(5,2)    NOT NULL CHECK (transit_days_mean > 0),
    transit_days_std    NUMERIC(5,2),
    cost_per_unit       NUMERIC(10,4)   NOT NULL CHECK (cost_per_unit >= 0),
    cost_per_lb         NUMERIC(10,4)
);

-- Clean service policy — z-scores from standard normal table, not made up
CREATE TABLE dim_service_policy (
    customer_segment        VARCHAR(20)     PRIMARY KEY CHECK (customer_segment IN ('PREMIUM', 'STANDARD', 'BUDGET')),
    target_fill_rate        NUMERIC(6,4)    NOT NULL CHECK (target_fill_rate BETWEEN 0 AND 1),
    z_score                 NUMERIC(6,4)    NOT NULL CHECK (z_score > 0),
    penalty_stockout_per_unit NUMERIC(10,2) NOT NULL,
    priority_rank           INTEGER         NOT NULL CHECK (priority_rank IN (1, 2, 3)),
    max_backorder_days      INTEGER,
    expedite_threshold      INTEGER,
    review_period_days      INTEGER,
    holding_cost_rate       NUMERIC(6,4),
    min_order_qty           INTEGER
);

-- =============================================================================
-- VERIFICATION QUERY — run after schema.sql to confirm all 14 tables exist
-- =============================================================================
-- SELECT table_name, 
--        (SELECT COUNT(*) FROM information_schema.columns 
--         WHERE table_name = t.table_name) as col_count
-- FROM information_schema.tables t
-- WHERE table_schema = 'public'
-- ORDER BY table_name;
-- Expected: 14 rows (7 raw + 7 clean)
