# KICKZ EMPIRE — ELT Pipeline

ELT (Extract, Load, Transform) pipeline for the **KICKZ EMPIRE** e-commerce website, built as part of the IMT Data Engineering course.

## Project Description
KICKZ EMPIRE is an e-commerce platform specializing in sneakers. The company is currently looking to use data to address strategic business issues (marketing, sales, products).

As a result, the company had accumulated a large amount of unstructured data, stored in various formats and lacking the proper framework to perform the analyses they needed.

To help them better access and utilize this information, this project proposes the development of an ELT pipeline based on the Medallion architecture (Bronze → Silver → Gold).

In summary, the final pipeline:
- extracts raw data from the S3 data lake
- loads the raw data into PostgreSQL (Bronze layer)
- cleans and transforms the data into structured tables (Silver layer)
- produces aggregated datasets (Gold layer)

##  Architecture

```
S3 (CSV / JSONL / Parquet)  ->  Bronze (raw) ->  Silver (clean)  -> Gold (analytics)
```

<!-- | Layer | Schema | Description |
|---|---|---|
| **Bronze** | `bronze_groupN` | Raw data — faithful copy of CSV files from S3 |
| **Silver** | `silver_groupN` | Cleaned data — internal columns removed, PII masked |
| **Gold** | `gold_groupN` | Aggregated data — ready for dashboards | -->

### In each layer: 
🥉 Bronze Layer
- Raw data ingested from S3
- No transformation applied

🥈 Silver Layer
- Data cleaning and validation
- Removal of internal columns and sensitive (PII) columns

🥇 Gold Layer
- Data aggregations
- Optimized for analytics and dashboards

## Project Structure

```
├── docs/
│   ├── DATA_PRESENTATION.md    # KICKZ EMPIRE data presentation
│   └── tp1/
│       └── INSTRUCTIONS.md     # Step-by-step TP1 instructions
├── src/
│   ├── __init__.py
│   ├── database.py             # PostgreSQL connection (AWS RDS)
│   ├── extract.py              # Extract: S3 (CSV) → Bronze
│   ├── transform.py            # Transform: Bronze → Silver
│   └── gold.py                 # Gold: Silver → Gold (aggregations)
├── pipeline.py                 # ELT orchestrator
├── tests/                      # Tests (Development TP3: tests for database, extract, transform, gold, conftest)
├── .env.example                # Environment variables template
├── .gitignore
├── requirements.txt
└── README.md
```

##  Setup instructions 
```bash
# 1. Setup

git clone <repo-url> #Clone the repository
python -m venv venv && source venv/bin/activate #Create a virtual environment
pip install -r requirements.txt #Installing dependencies
cp .env.example .env  #Configure environment variables

# 2. Test the database connection
python -m src.database
```

## How to run (full pipeline + individual steps)
```bash
# 1. Run Full Pipeline
python pipeline.py

# 2. Extract (S3 -> Bronze)
python pipeline.py --step extract

# 3. Transform (Bronze -> Silver)
python pipeline.py --step transform

# 4. # Gold (Silver -> Gold)
python pipeline.py --step gold

```

## How to test (pytest commands)
```bash

# 1. Run tests
pytest tests/ -v

# 2. Run with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# 3. Generate HTML report for tests
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

<!-- ## 📊 Datasets

| Dataset | Format | Source (S3) | Bronze Table |
|---|---|---|---|
| Product Catalog | CSV | `raw/catalog/products.csv` | `products` |
| Users | CSV | `raw/users/users.csv` | `users` |
| Orders | CSV | `raw/orders/orders.csv` | `orders` |
| Order Line Items | CSV | `raw/order_line_items/order_line_items.csv` | `order_line_items` | -->

<!-- ## 📚 Documentation

- [Data Presentation](docs/DATA_PRESENTATION.md)
- [TP1 Instructions](docs/tp1/INSTRUCTIONS.md) -->

<!-- ## ⚙️ Tech Stack

- **Python 3.10+** : Main language
- **pandas** : Data manipulation
- **boto3** : AWS S3 access
- **SQLAlchemy** : ORM / PostgreSQL connection
- **PostgreSQL** (AWS RDS) : Database
- **pytest** : Testing (TP2) -->

## Team members
- BEN AMAR Yasmine
- MARZOUGUI Mariem
- DE JESUS ARAGÃO Julia
