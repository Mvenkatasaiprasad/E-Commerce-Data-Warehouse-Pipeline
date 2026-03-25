# 🛒 E-Commerce Data Warehouse Pipeline

A end-to-end SQL-based Data Engineering pipeline built on 
Microsoft SQL Server following the Medallion Architecture 
(Bronze → Silver → Gold) with Power BI reporting.

---

## 📌 Project Overview

This project implements a full data pipeline using the 
**Medallion Architecture** to process the Olist Brazilian 
E-Commerce dataset from raw CSV files into business-ready 
analytical tables for Power BI dashboards.

**Pipeline Flow:**
```
Source (CSV) → Bronze → Silver → Gold → Power BI
```

---

## 🏗️ Architecture

![Architecture Diagram](docs/architecture.png)

| Layer | Description |
|-------|-------------|
| **Source** | Raw CSV files from Kaggle (Olist Dataset) |
| **Bronze** | Raw ingested data + metadata (ingestion_ts, source_system) |
| **Silver** | Cleaned, standardized, deduplicated data |
| **Gold** | Business-ready facts, dimensions & aggregated views |
| **Control** | Pipeline watermark & execution logging |

---

## 📂 Repository Structure
```
olist-dw-pipeline/
│
├── README.md
│
├── data_model/
│   ├── bronze.sql          -- Bronze layer tables
│   ├── silver.sql          -- Silver layer tables
│   └── gold.sql            -- Gold layer tables, views
│
├── stored_procedures/
│   └── sp.sql              -- All pipeline stored procedures
│
├── user_logging/
│   ├── watermark.sql       -- Watermark control table
│   ├── pipeline_log.sql    -- Pipeline logging table
│   └── user_logging.sql    -- Logging stored procedure
│
└── powerbi/
    └── Olist_Ecommerce_Dashboard.pbix
```

---

## 🗄️ Dataset

**Source:** Olist Brazilian E-Commerce Dataset  
**Download:** [Kaggle - Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| File | Table | Rows |
|------|-------|------|
| olist_customers_dataset.csv | source.customers | ~99K |
| olist_orders_dataset.csv | source.orders | ~99K |
| olist_order_items_dataset.csv | source.order_items | ~112K |
| olist_products_dataset.csv | source.products | ~32K |
| olist_order_payments_dataset.csv | source.payments | ~103K |

---

## 🔄 Pipeline Design

### Load Types

| Type | Description | When to Use |
|------|-------------|-------------|
| **Snapshot** | Full refresh — truncate + reload | First run / reset |
| **Incremental** | Only new records loaded | Subsequent runs |

### Stored Procedures

| Procedure | Schema | Purpose |
|-----------|--------|---------|
| `usp_load_bronze` | bronze | Source → Bronze load |
| `usp_load_silver` | silver | Bronze → Silver clean |
| `usp_load_gold` | gold | Silver → Gold build |
| `usp_run_pipeline` | dbo | Main orchestrator |

---

## 📊 Data Model

### Gold Layer

**Dimension Tables**
- `gold.dim_customers` — Customer details
- `gold.dim_products` — Product details

**Fact Tables**
- `gold.fact_orders` — Order transactions
- `gold.fact_sales` — Sales line items

**Aggregated Views**
- `gold.sales_summary` — Daily revenue & orders
- `gold.product_performance` — Revenue by category
- `gold.customer_revenue` — Revenue by customer

---

## ⚙️ Control Layer

| Table | Purpose |
|-------|---------|
| `control.pipeline_watermark` | Tracks last successful load timestamp |
| `control.pipeline_log` | Logs every pipeline run with status & row counts |

---

## 📈 Power BI Dashboard

Three dashboard pages built on Gold layer:

| Page | Visuals |
|------|---------|
| **Sales Overview** | Total Revenue, Total Orders, Avg Order Value, Revenue Trend |
| **Product Performance** | Revenue by Category, Orders by Category |
| **Customer Insights** | Customer Revenue Table, Revenue by State |

---

## 🚀 How to Run

### Prerequisites
- Microsoft SQL Server (Express or Developer edition)
- SQL Server Management Studio (SSMS)
- Power BI Desktop
- Kaggle dataset downloaded

### Steps

**1. Setup Database**
```sql
CREATE DATABASE OlistPipeline;
```

**2. Create Schemas**
```sql
CREATE SCHEMA source;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA control;
```

**3. Run SQL files in order**
```
1. user_logging/watermark.sql
2. user_logging/pipeline_log.sql
3. data_model/bronze.sql
4. data_model/silver.sql
5. data_model/gold.sql
6. stored_procedures/sp.sql
```

**4. Load Source Data**
```sql
BULK INSERT source.customers
FROM 'C:\YourPath\olist_customers_dataset.csv'
WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n');
-- Repeat for all 5 tables
```

**5. Run Pipeline**
```sql
-- Snapshot (first time)
EXEC dbo.usp_run_pipeline @load_type = 'snapshot';

-- Incremental (subsequent runs)
EXEC dbo.usp_run_pipeline @load_type = 'incremental';
```

**6. Check Logs**
```sql
SELECT * FROM control.pipeline_log ORDER BY log_id DESC;
SELECT * FROM control.pipeline_watermark;
```

**7. Open Power BI**
- Connect to `localhost\SQLEXPRESS`
- Database: `OlistPipeline`
- Load Gold layer tables/views

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Microsoft SQL Server | Database & pipeline execution |
| SSMS | SQL development & query execution |
| T-SQL | Stored procedures & transformations |
| Power BI Desktop | Dashboard & reporting |

---
