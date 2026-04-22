# MEIO OPTIMIZER — MASTER PROJECT CONTEXT
**Last Updated:** April 2025  
**Read this first in ANY new Claude/ChatGPT/Cursor session.**  
**This file = full context. No other file needed to resume work.**

---

## WHO IS JAYESH (30 seconds)

- **Name:** Jayesh Patel | Chicago, IL
- **Degree:** M.S. Industrial Technology & Operations, IIT Chicago — GPA 4.0, Dec 2025
- **Experience:** 6 years supply chain ops (India) — inventory management, procurement, BOM planning
- **Visa:** Requires H1B sponsorship. Only suggest companies that sponsor.
- **Target companies:** Amazon, Tesla, Walmart, Microsoft, Accenture SCM, Deloitte SCM, Oliver Wyman
- **Target roles:** Supply Chain Analyst, Supply Chain Data Scientist
- **Python level:** Beginner-intermediate. Reads/modifies code. Cannot write complex logic from scratch. Claude writes, Jayesh runs and learns.

---

## COMPLETED PROJECT

**Supplier Intelligence Platform** — live on Streamlit, code on GitHub  
Bayesian risk scoring (6 signals), SIR epidemic propagation, Monte Carlo VaR/CVaR (5,000+ iterations), graph centrality (PageRank, Betweenness, articulation points), TCO analysis, Claude API Sentinel Agent.  
Stack: Python, Streamlit, Plotly, NumPy, SciPy, NetworkX, SQLite.

---

## CURRENT PROJECT: MEIO OPTIMIZER

### What it is
A multi-echelon inventory optimization system. Starts from messy ERP-like data (7 tables with injected quality issues). Uses SQL + Python (Stockpyl/PuLP) + Streamlit to clean, model, and optimize a 3-tier supply network (DCs → RDCs → Stores). Outputs prescriptive decisions: reorder points, transfer plans, cost-to-serve by segment.

### 4 signals it must send recruiters
1. "He handles messy ERP data with SQL"
2. "He speaks service level / OTIF / working capital / cost-to-serve"
3. "He outputs ACTIONS — not just charts"
4. "This could plug into SAP IBP / Kinaxis / Blue Yonder"

### Tech stack
| Layer | Tools |
|-------|-------|
| Database | PostgreSQL local, DB: `meio_optimizer_db` |
| Analytics | Python 3.10+, Pandas, NumPy, SciPy |
| Optimization | Stockpyl (MEIO), PuLP (LP allocation) |
| Simulation | NumPy Monte Carlo, 5,000 iterations |
| Frontend | Streamlit (5 pages), Plotly |
| Export | openpyxl / xlsxwriter |

---

## FOLDER STRUCTURE

```
/meio-optimizer/
├── /data/
│   ├── /raw/               <- 7 original CSVs — NEVER MODIFY
│   └── /processed/         <- outputs from SQL cleaning
├── /sql/                   <- ALL 4 FILES COMPLETE
│   ├── schema.sql
│   ├── ingest.sql
│   ├── cleaning.sql
│   └── marts.sql
├── /src/                   <- Python modules — NOT YET BUILT
│   ├── optimizer.py        <- NEXT TO BUILD
│   ├── simulation.py
│   ├── cost_to_serve.py
│   └── prescriptive.py
├── /outputs/
├── app.py                  <- Streamlit app
├── requirements.txt
└── README.md
```

---

## THE 7 RAW CSV FILES

| File | Rows | Injected Issues |
|------|------|-----------------|
| `sku_master.csv` | 200 | 5 mixed SKU formats, 24 NULL lead_time_days, inconsistent active flags (Y/YES/y/1) |
| `locations.csv` | 28 | CLEAN. 6 DCs, 10 RDCs, 12 Stores. Echelon: DC=1, RDC=2, STORE=3 |
| `sales_orders.csv` | 500 | ~10 duplicate order_ids (ERP re-transmission bug) |
| `shipments.csv` | 400 | 30 NULL freight_cost_usd, 7 rows at 100x normal ($297K vs $2.97K) |
| `inventory_snapshots.csv` | 300 | 9 rows with negative on_hand_qty (WMS sync bug) |
| `lane_costs.csv` | 140 | CLEAN. DC->RDC and RDC->STORE lanes with distance, transit days, cost/unit, mode |
| `service_policy.csv` | 3 | CLEAN. PREMIUM z=2.33, STANDARD z=1.65, BUDGET z=1.28 |

---

## NETWORK STRUCTURE

```
Tier 1 (Echelon 1): 6 DCs      <- receives from suppliers
         |
Tier 2 (Echelon 2): 10 RDCs    <- regional distribution
         |
Tier 3 (Echelon 3): 12 Stores  <- fulfills customer orders
```

---

## SQL PIPELINE — COMPLETE

**Run order:** schema.sql -> ingest.sql -> cleaning.sql -> marts.sql

**ACTION NEEDED before ingest.sql:** Update COPY file paths to your local path.  
Mac: `/Users/jayesh/meio-optimizer/data/raw/filename.csv`  
Windows: `C:/Users/jayesh/meio-optimizer/data/raw/filename.csv`

### schema.sql
- 14 tables: 7 `_raw` staging (all TEXT) + 7 clean production (typed + FK-constrained)
- Raw tables accept dirty data without crashing. Clean tables enforce business rules.
- Key: `fact_inventory_snapshot` composite PK = `(snapshot_date, location_id, sku_id)`

### ingest.sql
- COPY commands for all 7 CSVs into raw staging tables
- Load order: dimensions first (location, sku, policy, lanes), then facts
- Verification query at bottom shows row counts for all 7 tables

### cleaning.sql — THE PORTFOLIO FILE
Every fix has a WHY comment explaining the business reasoning:

| Problem | Fix | Key reasoning |
|---------|-----|---------------|
| Mixed SKU formats (5 variants) | UPPER + REPLACE hyphens/underscores/spaces | All variants must resolve to same FK |
| NULL lead_time_days (24 rows) | Category-average imputation | Electronics avg != Bulk avg. Global avg breaks safety stock math |
| Inconsistent active flags | CASE WHEN UPPER(active) IN ('Y','YES','1','TRUE','T') | Handles all 5 variants |
| Duplicate order_ids (~10) | DISTINCT ON order_id, ORDER BY ctid DESC | MAX ctid = most recent ERP transmission = corrected version |
| Freight outliers (7 rows) | Winsorization at P99 per transport_mode | Cap not drop — dropping removes shipment record, breaks inventory balance |
| NULL freight_cost (30 rows) | Mode-level avg after capping | AIR != TRUCK != RAIL. Global avg understates premium segment cost |
| Negative on_hand_qty (9 rows) | GREATEST(on_hand_qty, 0) | Physical inventory cannot be negative. WMS sync lag. |

### marts.sql
5 analytical tables — Python reads these, not the raw fact tables:

| Mart | Python uses it for |
|------|--------------------|
| `mart_demand_stats` | Safety stock inputs: avg_demand_per_day, std_demand_per_day, safety_stock_units, reorder_point_units |
| `mart_inventory_position` | Current state: inventory_position, days_of_supply, stock_status, safety_stock_gap_usd |
| `mart_cost_to_serve` | Cost by segment/region/SKU: margin_pct, cost_per_unit, freight/carrying/stockout breakdown |
| `mart_network_flow` | Lane actuals: freight_cost_per_unit, avg_actual_lead_time_days for Monte Carlo |
| `mart_data_quality_report` | Streamlit Page 2 audit trail display |

Key formula already computed in mart_demand_stats:
```sql
safety_stock_units  = ROUND(z_score * std_demand_per_day * SQRT(lead_time_days))
reorder_point_units = ROUND((avg_demand_per_day * lead_time_days) + safety_stock_units)
```

Statistical filter: `HAVING COUNT(order_date) >= 30`  
Why: CLT minimum. Std dev below 30 observations has ~50% margin of error.

---

## PYTHON LAYER — NOT YET BUILT

### Next file: src/optimizer.py

Steps:
1. Connect to PostgreSQL via SQLAlchemy
2. Read mart_demand_stats and mart_inventory_position
3. Build multi-echelon network using Stockpyl
4. Run optimization (Graves & Willems)
5. Run LP allocation with PuLP
6. Output optimized_policy.csv

Output columns needed:
`location_id, sku_id, safety_stock_baseline, safety_stock_optimized, reorder_point_optimized, order_qty_optimized, expected_holding_cost, expected_stockout_cost, expected_fill_rate`

Core formulas:
```python
safety_stock = z_score * std_demand_per_day * np.sqrt(lead_time_days)
reorder_point = (avg_demand_per_day * lead_time_days) + safety_stock
holding_cost = safety_stock * unit_cost * 0.25  # 25% annual rate
total_cost = holding_cost + ordering_cost + stockout_cost + transport_cost
```

Connection string:
```python
engine = sqlalchemy.create_engine('postgresql://localhost/meio_optimizer_db')
demand_stats = pd.read_sql('SELECT * FROM mart_demand_stats', engine)
```

### src/simulation.py
- Monte Carlo: 5,000 iterations x 365 days
- Demand from lognormal (not normal) — real demand is right-skewed
- Outputs: VaR(95%), CVaR(95%), P90 cost, fill rate distribution
- Compare baseline vs optimized policy

### src/prescriptive.py
- Top 10 actions ranked by net annual value
- Types: safety_stock_adjustment, transfer, service_policy_change, repricing
- Each has: action (plain English), net_annual_value, priority (HIGH/MEDIUM)

### src/cost_to_serve.py
- Reads mart_cost_to_serve
- Flags segments with margin_pct < 15%
- Outputs repricing recommendations

---

## STREAMLIT APP — 5 PAGES

| Page | Key elements |
|------|-------------|
| 1 Executive Dashboard | 4 KPI cards, network graph, before/after table |
| 2 Data Quality Report | 7 issues found/fixed, SQL pipeline, data lineage |
| 3 Optimization Results | Policy table, cost breakdown chart, Monte Carlo histogram |
| 4 Cost-to-Serve | Segment profitability, cost waterfall, repricing alerts |
| 5 Prescriptive Actions | Top 10 actions ranked by net value |

---

## TARGET RESULTS (case study PDF)

| Metric | Baseline | Optimized | Delta |
|--------|----------|-----------|-------|
| Network Inventory | $8.9M | $7.6M | -14.6% |
| Fill Rate (OTIF) | 92.1% | 95.8% | +3.7 pp |
| Stockout Events/Year | 127 | 48 | -62% |
| Annual Network Cost | $8.9M | $7.6M | -14.6% |

Working capital freed: $1.3M x 10% cost of capital = $130K/yr finance savings.

---

## KEY DESIGN DECISIONS (explain in interviews)

1. **Two-layer schema:** Never destroy source data. Cleaning is a transform, not a delete.
2. **Category-level lead time imputation:** Global average hides category differences. Wrong imputation cascades into wrong safety stock.
3. **MAX(ctid) for dedup:** Most recent ERP transmission is the correction. Keep the fix, not the error.
4. **Winsorization over deletion:** Deleting freight outliers removes qty_shipped from the ledger. Inventory balance breaks.
5. **GREATEST(on_hand, 0):** WMS sync lag creates phantom negatives. Safety stock math breaks with negative inputs.
6. **HAVING COUNT >= 30:** CLT minimum. Below 30 = exclude from optimization, flag for manual review.
7. **Lognormal demand:** Real demand is right-skewed. Normal distribution underestimates tail risk.
8. **Mode-level freight imputation:** AIR >> TRUCK >> RAIL. Global average understates premium segment cost-to-serve.

---

## ACADEMIC REFERENCES

| Reference | Used for |
|-----------|---------|
| Clark & Scarf (1960) | Single-location safety stock formula foundation |
| Graves & Willems (2000) | Guaranteed-service MEIO model (Stockpyl implements this) |
| Ballou (2004) | 25% annual holding cost rate benchmark |
| Chopra & Sodhi (2004) | Supply chain risk framework |

---

## HOW TO WORK WITH JAYESH

- Ask 2-3 clarifying questions before writing code unless task is clear or he says "just do it"
- Never use paid APIs without asking first
- Create files for code >20 lines — no walls of text in chat
- Add comments explaining what each block does — he learns by reading real code
- Push back directly if something is wrong — he wants to be challenged
- Pareto filter every suggestion: does this make the project more impressive, or just more complex?
- H1B context: only suggest companies that sponsor. No startups under 100 employees.

---

## CURRENT STATUS

```
WEEK 1 — DATA ENGINEERING
[DONE] Raw CSV data — 7 files in data/raw/
[DONE] schema.sql  — 14 tables, two-layer architecture
[DONE] ingest.sql  — COPY commands for all 7 CSVs
[DONE] cleaning.sql — 7 data quality fixes, all documented
[DONE] marts.sql   — 5 analytical tables for Python optimizer

WEEK 2 — OPTIMIZATION CORE
[TODO] src/optimizer.py    <- START HERE
[TODO] src/simulation.py
[TODO] src/cost_to_serve.py
[TODO] outputs/optimized_policy.csv

WEEK 3 — PRESCRIPTIVE ENGINE
[TODO] src/prescriptive.py
[TODO] outputs/recommendations.csv
[TODO] outputs/MEIO_Planning_Export.xlsx

WEEK 4 — DASHBOARD + CASE STUDY
[TODO] app.py (Streamlit, 5 pages)
[TODO] outputs/case_study_amazon.pdf
[TODO] outputs/case_study_tesla.pdf
[TODO] README.md
[TODO] GitHub repo + Streamlit Cloud deploy
```

---

## EXACT PROMPT TO RESUME IN A NEW SESSION

Copy-paste this into any AI tool to resume:

```
Read MEIO_PROJECT_CONTEXT.md in the /meio-optimizer/ folder.
This is my supply chain portfolio project. The SQL pipeline (Week 1) is complete.
I need to build src/optimizer.py next — the Python MEIO optimization layer.

Start by:
1. Confirming you understand the project from the context file
2. Asking 2-3 clarifying questions before writing any code
3. Then build the SQLAlchemy connection + Stockpyl network scaffold
```
