# E-Commerce Data Platform on Databricks

A medallion-architecture (Bronze / Silver / Gold) data pipeline built on Databricks that ingests raw e-commerce CSV data, cleanses and transforms it through multiple layers, and produces BI-ready dimensional and fact tables for analytics and dashboarding.

## Table of Contents

* [Architecture Overview](#architecture-overview)
* [Databricks Architecture & Workflow](#databricks-architecture--workflow)
* [Project Structure](#project-structure)
* [Setup](#setup)
* [Dimension Pipeline (medallion\_processing\_dim)](#dimension-pipeline-medallion_processing_dim)
* [Fact Pipeline (medallion\_processing\_fact)](#fact-pipeline-medallion_processing_fact)
* [BI / Dashboard Layer](#bi--dashboard-layer)
* [Data Lineage Summary](#data-lineage-summary)
* [How to Run](#how-to-run)

## Architecture Overview

This project implements the **Medallion Architecture** — a layered data design pattern that progressively improves data quality and structure as data flows through three tiers:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        RAW CSV FILES (UC Volume)                         │
│              /Volumes/ecommerce/source_data/raw/*                         │
│  brands │ category │ products │ customers │ date │ order_items            │
└───────────────────────────────┬──────────────────────────────────────────┘
                                  │
                     Bronze Layer (Raw Ingestion)
                     ─────────────────────────────
                     • Schema-enforced CSV reads
                     • Metadata columns (source file, ingest timestamp)
                     • Overwrite mode Delta tables
                                  │
                     Silver Layer (Cleansed & Conformed)
                     ─────────────────────────────
                     • Deduplication
                     • String trimming & regex cleansing
                     • Anomaly correction (e.g., "Two" → 2)
                     • Data type casting (string → int/double/date/timestamp)
                     • Channel normalization (web → Website, app → Mobile)
                                  │
                     Gold Layer (BI-Ready Analytical Models)
                     ─────────────────────────────
                     • Dimension tables (products, customers, date)
                     • Fact table with computed measures
                     • Currency conversion to INR
                     • Denormalized view for dashboards
                                  │
                     ┌──────────┴──────────┐
                     │  AI/BI Dashboards &   │
                     │  SQL Analytics        │
                     └─────────────────────┘
```

## Databricks Architecture & Workflow

### Unity Catalog (Data Governance)

All data is governed under a single Unity Catalog named **`ecommerce`** with three schemas representing the medallion layers:

| Schema | Purpose | Naming Convention |
| --- | --- | --- |
| `ecommerce.bronze` | Raw ingestion tables | `brz_<entity>` |
| `ecommerce.silver` | Cleansed, conformed data | `slv_<entity>` |
| `ecommerce.gold` | BI-ready dimension & fact tables | `gld_dim_<entity>`, `gld_fact_<entity>` |

### Compute

The pipeline runs on **serverless compute** (auto-selected). No dedicated cluster configuration is required — Databricks automatically provisions compute when notebook cells are executed.

### Storage

Source data resides in a **Unity Catalog Volume** at `/Volumes/ecommerce/source_data/raw/`. Each entity has its own subdirectory containing CSV files:

* `brands/brands.csv`
* `category/category.csv`
* `products/products.csv`
* `customers/customers.csv`
* `date/date.csv`
* `order_items/landing/order_items_*.csv` (partitioned by date)

### Delta Lake

All Bronze, Silver, and Gold tables are written as **Delta Lake** managed tables using `saveAsTable()` with `mode("overwrite")` and `mergeSchema` enabled. Delta Lake provides ACID transactions, schema enforcement, and time travel capabilities.

### Execution Model

Each layer is implemented as a separate Databricks notebook. Notebooks are executed cell-by-cell in sequence. The two processing tracks (dimension and fact) are independent and can run in parallel, but each track must follow Bronze → Silver → Gold order since each layer reads from the previous one.

## Project Structure

```
DataBricks/
├── README.md                                  ← This file
├── Query_to_dashboaard                        ← SQL view for BI dashboards
├── setup/
│   └── New Notebook 2026-09-15 14:17:27       ← Catalog & schema creation
├── medallion_processing_dim/                  ← Dimension data pipeline
│   ├── dim_bronze                             ← Ingest raw dimension CSVs
│   ├── dim_silver                             ← Cleanse dimension data
│   └── dim_gold                               ← Build Gold dimension tables
└── medallion_processing_fact/                 ← Fact data pipeline
    ├── 1_fact_bronze                          ← Ingest raw order_items CSVs
    ├── 2_fact_silver                          ← Cleanse & transform orders
    └── 3_fact_gold                             ← Build Gold fact table with measures
```

## Setup

**Notebook:** `setup/New Notebook 2026-09-15 14:17:27`

This notebook initializes the Unity Catalog infrastructure. It must be run once before any pipeline notebooks.

Steps performed:

1. Creates the `ecommerce` catalog if it does not exist
2. Sets `ecommerce` as the active catalog
3. Creates three schemas: `ecommerce.bronze`, `ecommerce.silver`, `ecommerce.gold`
4. Verifies the schemas with `SHOW DATABASES`

## Dimension Pipeline (medallion_processing_dim)

### dim\_bronze — Raw Ingestion

**Notebook:** `medallion_processing_dim/dim_bronze`

Reads raw CSV files from the UC Volume with explicit `StructType` schemas and writes them to Bronze Delta tables. Each table includes metadata columns (`_source_file`, `ingested_at` / `ingest_timestamp`) for traceability.

| Source CSV | Bronze Table | Key Columns |
| --- | --- | --- |
| `brands.csv` | `bronze.brz_brands` | `brand_code`, `brand_name`, `category_code` |
| `category.csv` | `bronze.brz_category` | `category_code`, `category_name` |
| `products.csv` | `bronze.brz_products` | `product_id`, `sku`, `category_code`, `brand_code`, `color`, `size`, `material`, `weight_grams`, `length_cm`, `width_cm`, `height_cm`, `rating_count` |
| `customers.csv` | `bronze.brz_customers` | `customer_id`, `phone`, `country_code`, `country`, `state` |
| `date.csv` | `bronze.brz_date` | `date`, `year`, `day_name`, `quarter`, `week_of_year` |

Notable design decisions:

* Some numeric fields (`weight_grams`, `length_cm`) are intentionally read as `StringType` because the incoming data contains anomalies that would cause cast failures
* The `_metadata.file_path` column is captured for full source-file lineage

### dim\_silver — Cleansing & Conformance

**Notebook:** `medallion_processing_dim/dim_silver`

Reads each Bronze table, applies data quality transformations, and writes to Silver Delta tables.

| Bronze Table | Silver Table | Cleansing Operations |
| --- | --- | --- |
| `brz_brands` | `slv_brands` | Trim whitespace on `brand_name`; strip non-alphanumeric characters from `brand_code`; normalize anomalous `category_code` values (GROCERY→GRCY, BOOKS→BKS, TOYS→TOY) |
| `brz_category` | `slv_category` | Remove duplicate `category_code` rows; standardize category codes to uppercase |
| `brz_products` | `slv_products` | Cast anomalous string columns to proper numeric types; clean SKU and product attributes |
| `brz_customers` | `slv_customers` | Deduplicate by `customer_id`; normalize phone, country, and state fields |
| `brz_date` | `slv_date` | Validate date formats; correct negative `week_of_year` values; standardize `day_name` casing |

### dim\_gold — BI-Ready Dimension Tables

**Notebook:** `medallion_processing_dim/dim_gold`

Joins Silver tables to produce enriched Gold dimension tables suitable for star-schema analytics.

**`gold.gld_dim_products`** — Built via SQL `CREATE OR REPLACE TABLE`:

* Joins `slv_products` with `slv_brands` and `slv_category` on `category_code` and `brand_code`
* Uses `COALESCE` to handle missing brand/category names with "Not Available" fallbacks
* Produces a single enriched product dimension with all descriptive attributes

**`gold.gld_dim_customers`** — Built via PySpark:

* Enriches customer data with a country-to-state-to-region mapping
* India states are mapped to regions (West, South, North)
* Australia states are mapped to regions (SouthEast, West, East)
* The mapping is flattened from Python dictionaries into a Spark DataFrame and joined to customer records

**`gold.gld_dim_date`** — Built from the cleansed date dimension with derived calendar attributes (`is_weekend`, `month_name`, etc.)

## Fact Pipeline (medallion_processing_fact)

### 1\_fact\_bronze — Raw Order Ingestion

**Notebook:** `medallion_processing_fact/1_fact_bronze`

Ingests daily order item CSV files from `/Volumes/ecommerce/source_data/raw/order_items/landing/`.

* Reads all CSV files in the landing directory with a defined schema (13 columns, all `StringType` to tolerate anomalies)
* Adds `file_name` and `ingest_timestamp` metadata columns
* Writes to `bronze.brz_order_items` as a Delta table

Key columns: `dt`, `order_ts`, `customer_id`, `order_id`, `item_seq`, `product_id`, `quantity`, `unit_price_currency`, `unit_price`, `discount_pct`, `tax_amount`, `channel`, `coupon_code`

### 2\_fact\_silver — Cleansing & Transformation

**Notebook:** `medallion_processing_fact/2_fact_silver`

Reads `bronze.brz_order_items` and applies a series of transformations:

1. **Deduplication** — Removes duplicate rows by `order_id` + `item_seq`
2. **Quantity normalization** — Converts textual values like "Two" to numeric `2`, then casts to `int`
3. **Price cleansing** — Strips `$` symbols from `unit_price`, casts to `double`
4. **Discount parsing** — Removes `%` suffix from `discount_pct`, casts to `double`
5. **Coupon normalization** — Lowercases and trims `coupon_code`
6. **Channel standardization** — Maps `web` → `Website`, `app` → `Mobile`
7. **Type conversions** — `dt` → `date`, `order_ts` → `timestamp` (with dual-format parsing), `item_seq` → `int`, `tax_amount` → `double`
8. Adds `processed_time` column

Writes to `silver.slv_order_items`

### 3\_fact\_gold — Fact Table with Computed Measures

**Notebook:** `medallion_processing_fact/3_fact_gold`

Reads `silver.slv_order_items` and enriches it with calculated financial measures and currency conversion.

**Computed measures:**

* `gross_amount` = `quantity` × `unit_price`
* `discount_amount` = `gross_amount` × (`discount_pct` / 100)
* `sale_amount` = `gross_amount` − `discount_amount` + `tax_amount`
* `date_id` = integer representation of date (e.g., `20250801`)
* `coupon_flag` = 1 if coupon code is present, else 0

**Currency conversion:**

Uses fixed FX rates (as of 2025-10-15) to convert all sale amounts to INR:

| Currency | Rate (INR) |
| --- | --- |
| INR | 1.00 |
| AED | 24.18 |
| AUD | 57.55 |
| CAD | 62.93 |
| GBP | 117.98 |
| SGD | 68.18 |
| USD | 88.29 |

The `sale_amount_inr` column is computed by joining the FX rates DataFrame and multiplying `sale_amount` × `inr_rate`, then rounding up with `F.ceil()`.

Writes the final curated fact table to `gold.gld_fact_order_items` with columns: `date_id`, `transaction_date`, `transaction_ts`, `transaction_id`, `customer_id`, `seq_no`, `product_id`, `channel`, `coupon_code`, `coupon_flag`, `unit_price_currency`, `quantity`, `unit_price`, `gross_amount`, `discount_pct`, `discount_amount`, `tax_amount`, `sale_amount`, `sale_amount_inr`.

## BI / Dashboard Layer

**File:** `Query_to_dashboaard`

Creates a denormalized view that joins the Gold fact table with Gold dimension tables for direct consumption by AI/BI dashboards.

```sql
CREATE OR REPLACE VIEW ecommerce.gold.fact_transactions_denorm AS (
  SELECT i.*, c.year, c.month_name, c.day_name, c.is_weekend,
         c.quarter, c.week, p.sku, p.category_code, p.category_name,
         p.brand_code, p.brand_name, p.color, p.size, p.rating_count,
         extract(HOUR FROM transaction_ts) as hour_of_day
  FROM ecommerce.gold.gld_fact_order_items i
  JOIN ecommerce.gold.gld_dim_date c       ON i.date_id = c.date_id
  JOIN ecommerce.gold.gld_dim_products p   ON i.product_id = p.product_id
);
```

This view enables dashboard queries by date attributes (year, month, quarter, weekday/weekend), product attributes (category, brand, color, size), and time-of-day analysis without requiring runtime joins.

## Data Lineage Summary

```
RAW CSVs (UC Volume)
    │
    ├──[dim_bronze]──→ bronze.brz_brands, brz_category, brz_products, brz_customers, brz_date
    │                        │
    │                   [dim_silver]──→ silver.slv_brands, slv_category, slv_products, slv_customers, slv_date
    │                        │
    │                   [dim_gold]──→ gold.gld_dim_products, gld_dim_customers, gld_dim_date
    │
    └──[1_fact_bronze]──→ bronze.brz_order_items
                              │
                         [2_fact_silver]──→ silver.slv_order_items
                              │
                         [3_fact_gold]──→ gold.gld_fact_order_items
                              │
                         [Query_to_dashboaard]──→ gold.fact_transactions_denorm (view)
                              │
                         AI/BI Dashboards
```

## How to Run

1. **Run the setup notebook first** — `setup/New Notebook 2026-09-15 14:17:27` creates the `ecommerce` catalog and `bronze` / `silver` / `gold` schemas.

2. **Run the dimension pipeline** in order:
   * `medallion_processing_dim/dim_bronze`
   * `medallion_processing_dim/dim_silver`
   * `medallion_processing_dim/dim_gold`

3. **Run the fact pipeline** in order:
   * `medallion_processing_fact/1_fact_bronze`
   * `medallion_processing_fact/2_fact_silver`
   * `medallion_processing_fact/3_fact_gold`

4. **Run the BI view** — Execute the SQL in `Query_to_dashboaard` to create the denormalized view.

5. **Build dashboards** — Query `ecommerce.gold.fact_transactions_denorm` in AI/BI dashboards for analytics.

> The dimension and fact pipelines are independent of each other and can be run in parallel. However, within each pipeline, Bronze must complete before Silver, and Silver before Gold.

## References

* [E-Commerce Data Pipeline Tutorial — YouTube](https://www.youtube.com/watch?v=761SQ9Hxbic&t=2951s)

