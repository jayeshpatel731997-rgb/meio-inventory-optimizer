# 📦 MEIO Decision Intelligence Dashboard

An interactive inventory optimization and decision-support tool built with **PostgreSQL, Python, and Streamlit**.  
This project simulates **service-level changes**, recalculates **safety stock** and **reorder points**, estimates **inventory cost tradeoffs**, and highlights **high-risk SKUs** across SKU-location combinations.

## 🚀 Live App
Streamlit deployment: `https://meio-inventory-optimizer-kvasgjglbaqvy77xz9ietu.streamlit.app`

---

## 🎯 Business Problem

Supply chain teams do not just need dashboards that show historical numbers. They need tools that help answer questions like:

- What happens if we increase service level from 95% to 98%?
- How much additional safety stock will be required?
- Which SKUs create the highest holding-cost burden?
- Which SKU-location combinations are most at risk?

This project was built to move from a **static inventory dashboard** to a **decision intelligence tool** for inventory planning.

---

## 🧠 What the App Does

The dashboard allows users to:

- adjust **target service level**
- adjust **holding cost per unit**
- adjust **stockout cost per unit**
- filter by **SKU** and **location**
- compare **current vs simulated inventory policy**
- identify **top costliest SKUs**
- identify **high-risk SKUs**
- export scenario results as CSV

---

## ✨ Key Features

### 1. Interactive optimization controls
Users can simulate different inventory strategies with:
- Service Level (%)
- Holding Cost per Unit ($)
- Stockout Cost per Unit ($)

### 2. Dynamic inventory logic
The app recalculates:
- simulated safety stock
- simulated reorder point
- holding cost
- risk cost
- total estimated cost

### 3. Executive summary
A top KPI section shows:
- filtered SKUs
- average simulated safety stock
- average simulated reorder point
- total estimated cost

### 4. Decision insights
The app surfaces:
- **Top 5 Costliest SKUs**
- **High Risk SKUs**
- **Current vs Simulated Policy**

### 5. Exportable scenario output
Users can download the simulated inventory policy for further analysis.

---

## 🏗️ Tech Stack

- **SQL / PostgreSQL** — data pipeline and marts
- **Python** — optimization logic and simulation
- **Pandas** — data transformation
- **Streamlit** — interactive dashboard
- **GitHub + Streamlit Cloud** — deployment

---

## 📂 Project Structure

```text
MEIO/
├── meio-optimizer/
│   ├── app.py
│   ├── sample_data.csv
│   ├── requirements.txt
│   ├── data/
│   ├── outputs/
│   ├── sql/
│   └── src/
├── marts.sql
├── cleaning.sql
├── ingest.sql
├── schema.sql
└── meio.py