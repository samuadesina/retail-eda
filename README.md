# 🛍️ P01 — Retail EDA Pipeline

> End-to-end data engineering project: extracting 50,000+ retail transactions from a live Supabase database, transforming through a validated ETL pipeline, and producing a clean dataset ready for exploratory data analysis.

---

## 📌 Project Overview

**Client:** ShopSmart Retail Group (simulated)  
**Role:** Junior Data Analyst  
**Stack:** Python · PostgreSQL · Supabase · pandas · SQLAlchemy  
**Dataset:** 5 tables · 5.7M+ raw rows · 50,243 transactions extracted  

This project demonstrates a production-grade ETL pipeline built from scratch — from raw relational data in Supabase through to a validated, analysis-ready CSV. The EDA phase (revenue performance, discount impact, anomaly detection) is delivered in a separate module.

---

## 🏗️ Architecture

```
Supabase (PostgreSQL)
    │
    │  psycopg2 / SQLAlchemy
    ▼
sql/extracted_raw_data.sql      ← Joins 4 tables, computes window functions
    │
    ▼
data/raw/raw-data.csv           ← 50,243 rows · 26 columns
    │
    ├── DataValidator            ← Null checks · Duplicate detection · Range validation
    │
    ├── DataTransformer          ← Fill nulls · Fix types · Derive columns
    │
    ▼
data/processed/processed-data.csv   ← Clean, enriched, analysis-ready
```

---

## 📂 Project Structure

```
p01-retail-eda/
│
├── config.py                   # Central config: paths, logger, thresholds
├── run.py                      # Pipeline entry point — run this
│
├── sql/
│   └── extracted_raw_data.sql  # CTE-based extraction query with window functions
│
├── src/
│   ├── data_access/
│   │   ├── query_runner.py     # SQLAlchemy query executor
│   │   └── data_extractor.py  # CSV writer
│   │
│   └── etl/
│       ├── etl_pipeline.py     # Orchestrator — Extract → Validate → Transform → Load
│       ├── validator.py        # DataValidator: quality inspection
│       └── transformer.py      # DataTransformer: cleaning and enrichment
│
├── data/
│   ├── raw/                    # raw-data.csv (gitignored)
│   └── processed/              # processed-data.csv (gitignored)
│
├── .env.example                # Environment variable template
├── requirements.txt
└── README.md
```

---

## 🗄️ Source Schema

| Table | Rows | Role |
|---|---|---|
| `retail.sales` | 5.7M | Fact table — transactions |
| `retail.products` | 200K | Dimension — product catalogue |
| `retail.stores` | 24K | Dimension — store locations |
| `retail.returns` | 576K | Dimension — return records |
| `retail.inventory` | 864K | Dimension — stock levels |

---

## ⚙️ ETL Pipeline

### Extract
- Connects to Supabase via `psycopg2` using a direct PostgreSQL connection
- Executes a CTE-based SQL query joining `sales → products → stores → returns`
- Window functions compute `running_revenue` and `rank_within_category` at source
- Raw output: **50,243 rows × 26 columns**

### Validate
`DataValidator` runs 5 checks before any transformation:
- ✅ Non-empty check — fail fast if 0 rows returned
- ✅ Null audit — flags columns exceeding thresholds (LEFT JOIN nulls excluded)
- ✅ Duplicate detection — flags exact copy rows
- ✅ Numeric range check — catches impossible negative values
- ✅ Stats summary — row count, memory usage, issue counts

### Transform
`DataTransformer` applies targeted fixes:
- Fills return-related nulls (`N/A` / `0.0`) — expected from LEFT JOIN
- Parses `sale_date` to datetime
- Derives `profit` and `profit_margin_pct` columns
- Drops exact duplicate rows

### Load
- Writes `processed-data.csv` to `data/processed/`
- Logs file size, row count, date range, and remaining nulls

---

## Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/samueladesina/p01-retail-eda.git
cd p01-retail-eda
```

### 2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment
```bash
cp .env.example .env
# Edit .env and add your Supabase credentials
```

```env
DB_URL=postgresql://postgres:<password>@db.<project-ref>.supabase.co:5432/postgres
INDUSTRY=retail
```

### 5. Run the pipeline
```bash
python run.py
```

**Expected output:**
```
STEP 1 — EXTRACTING DATA FROM SUPABASE
✅ Saved 50,243 rows to data/raw/raw-data.csv

STEP 2 — RUNNING ETL PIPELINE
[VALIDATE] Row count: 50,243 rows ✓
[VALIDATE] Null check passed ✅
[VALIDATE] Duplicate check: 0 duplicate rows ✓
[PIPELINE] Transformation complete
✅ Pipeline complete → data/processed/processed-data.csv
```

---

## 📦 Dependencies

```
psycopg2-binary==2.9.9
pandas==2.2.2
sqlalchemy==2.0+
python-dotenv==1.0.1
numpy==1.26.4
scipy==1.13.0
```

---

## 🔐 Environment Variables

| Variable | Description |
|---|---|
| `DB_URL` | Full PostgreSQL connection string from Supabase |
| `INDUSTRY` | Schema prefix (default: `retail`) |
| `LEARNER_SCHEMA` | Learner-specific schema identifier |

Never commit your `.env` file. Use `.env.example` as a template.

---

## 📊 What's Next

The processed dataset feeds directly into **P01-EDA** (separate module):

- **Question 1 — Revenue performance:** Which categories and stores generate the highest revenue per sale? Which stores underperform vs category average?
- **Question 2 — Discount impact:** Does higher `discount_pct` correlate with higher `total_amount`? (Pearson correlation)
- **Question 3 — Anomaly detection:** IQR + Z-score consensus to flag unusual transactions → `reports/anomalies.csv`

---

## 👨🏾‍💻 Author

**Samuel Adesina**  
Data Analyst | Python · SQL · Data Engineering  
[GitHub](https://github.com/samueladesina) · ([LinkedIn](https://www.linkedin.com/in/samuadesina/))

---

