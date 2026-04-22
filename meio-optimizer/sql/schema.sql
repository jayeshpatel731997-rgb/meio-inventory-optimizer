-- =============================================================================
-- MEIO OPTIMIZER - schema.sql
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
-- RAW STAGING TABLES (all TEXT - exact 1:1 match to CSV headers)
-- IMPORTANT: These tables mirror the raw files exactly.
-- =============================================================================

-- Raw SKU master - exact columns from data/raw/sku_master.csv
CREATE TABLE dim_sku_raw (
    sku_id              TEXT,
    sku_name            TEXT,
    category            TEXT,
    unit_cost_usd       TEXT,
    lead_time_days      TEXT,
    supplier_id         TEXT,
    unit_weight_lbs     TEXT,
    shelf_life_days     TEXT,
    reorder_point       TEXT,
    min_order_qty       TEXT,
    segment             TEXT,
    active              TEXT
);

-- Raw locations - exact columns from data/raw/locations.csv
CREATE TABLE dim_location_raw (
    location_id             TEXT,
    location_name           TEXT,
    city                    TEXT,
    state                   TEXT,
    location_type           TEXT,
    region                  TEXT,
    capacity_units          TEXT,
    operating_cost_per_day  TEXT
);

-- Raw sales orders - exact columns from data/raw/sales_orders.csv
CREATE TABLE fact_sales_orders_raw (
    order_id                TEXT,
    order_date              TEXT,
    customer_id             TEXT,
    sku_id                  TEXT,
    location_id             TEXT,
    quantity                TEXT,
    unit_price_usd          TEXT,
    order_status            TEXT,
    channel                 TEXT,
    promised_delivery_date  TEXT
);

-- Raw shipments - exact columns from data/raw/shipments.csv
CREATE TABLE fact_shipments_raw (
    shipment_id         TEXT,
    order_id            TEXT,
    origin_loc_id       TEXT,
    dest_loc_id         TEXT,
    sku_id              TEXT,
    quantity_shipped    TEXT,
    ship_date           TEXT,
    arrival_date        TEXT,
    freight_cost_usd    TEXT,
    carrier             TEXT,
    shipment_status     TEXT,
    weight_lbs          TEXT,
    pallet_count        TEXT
);

-- Raw inventory snapshots - exact columns from data/raw/inventory_snapshots.csv
CREATE TABLE fact_inventory_snapshot_raw (
    snapshot_id         TEXT,
    snapshot_date       TEXT,
    location_id         TEXT,
    sku_id              TEXT,
    on_hand_qty         TEXT,
    on_order_qty        TEXT,
    reserved_qty        TEXT,
    available_qty       TEXT,
    snapshot_source     TEXT,
    days_of_supply      TEXT
);

-- Raw lane costs - exact columns from data/raw/lane_costs.csv
CREATE TABLE dim_lane_cost_raw (
    lane_id             TEXT,
    origin_location_id  TEXT,
    dest_location_id    TEXT,
    distance_miles      TEXT,
    transit_days        TEXT,
    cost_per_unit_usd   TEXT,
    cost_per_lb_usd     TEXT,
    carrier_mode        TEXT,
    contract_type       TEXT
);

-- Raw service policy - exact columns from data/raw/service_policy.csv
CREATE TABLE dim_service_policy_raw (
    segment                     TEXT,
    target_fill_rate_pct        TEXT,
    target_cycle_service_lvl    TEXT,
    max_lead_time_days          TEXT,
    safety_stock_multiplier     TEXT,
    review_period_days          TEXT,
    order_up_to_days            TEXT,
    carrying_cost_rate_annual   TEXT,
    stockout_penalty_per_unit   TEXT,
    notes                       TEXT
);


-- =============================================================================
-- CLEAN PRODUCTION TABLES (typed, constrained, FK-enforced)
-- =============================================================================

-- Clean SKU master - sku_id standardized to UPPER, no hyphens/underscores
CREATE TABLE dim_sku (
    sku_id              VARCHAR(50)     PRIMARY KEY,
    description         VARCHAR(200)    NOT NULL,
    category            VARCHAR(50)     NOT NULL,
    subcategory         VARCHAR(50),
    unit_cost           NUMERIC(10,2)   NOT NULL CHECK (unit_cost > 0),
    unit_volume_cuft    NUMERIC(10,4),
    unit_weight_lbs     NUMERIC(10,4),
    supplier_id         VARCHAR(50),
    lead_time_days      INTEGER         NOT NULL,
    reorder_point       INTEGER,
    active              BOOLEAN         NOT NULL DEFAULT TRUE,
    last_updated        DATE
);

-- Clean location master - network spine, every fact table joins here
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

-- Clean sales orders - deduped, typed, FK-enforced
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

-- Clean shipments - freight outliers capped, NULLs imputed
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

-- Clean inventory snapshots - negative on_hand floored to 0
CREATE TABLE fact_inventory_snapshot (
    snapshot_date       DATE            NOT NULL,
    location_id         VARCHAR(50)     NOT NULL REFERENCES dim_location(location_id),
    sku_id              VARCHAR(50)     NOT NULL REFERENCES dim_sku(sku_id),
    on_hand_qty         INTEGER         NOT NULL CHECK (on_hand_qty >= 0),
    on_order_qty        INTEGER         DEFAULT 0 CHECK (on_order_qty >= 0),
    backorder_qty       INTEGER         DEFAULT 0 CHECK (backorder_qty >= 0),
    safety_stock_qty    INTEGER         CHECK (safety_stock_qty >= 0),
    reorder_point_qty   INTEGER,
    last_receipt_date   DATE,
    last_issue_date     DATE,
    PRIMARY KEY (snapshot_date, location_id, sku_id)
);

-- Clean lane costs - freight engine for cost-to-serve
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

-- Clean service policy - z-scores from standard normal table, not made up
CREATE TABLE dim_service_policy (
    customer_segment          VARCHAR(20)     PRIMARY KEY CHECK (customer_segment IN ('PREMIUM', 'STANDARD', 'BUDGET')),
    target_fill_rate          NUMERIC(6,4)    NOT NULL CHECK (target_fill_rate BETWEEN 0 AND 1),
    z_score                   NUMERIC(6,4)    NOT NULL CHECK (z_score > 0),
    penalty_stockout_per_unit NUMERIC(10,2)   NOT NULL,
    priority_rank             INTEGER         NOT NULL CHECK (priority_rank IN (1, 2, 3)),
    max_backorder_days        INTEGER,
    expedite_threshold        INTEGER,
    review_period_days        INTEGER,
    holding_cost_rate         NUMERIC(6,4),
    min_order_qty             INTEGER
);

-- =============================================================================
-- VERIFICATION QUERY - run after schema.sql to confirm all 14 tables exist
-- =============================================================================
-- SELECT table_name,
--        (SELECT COUNT(*)
--         FROM information_schema.columns
--         WHERE table_name = t.table_name) AS col_count
-- FROM information_schema.tables t
-- WHERE table_schema = 'public'
-- ORDER BY table_name;
-- Expected: 14 rows (7 raw + 7 clean)
