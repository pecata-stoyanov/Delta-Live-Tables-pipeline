# **Databricks Delta Live Tables – Medallion Architecture Pipeline**

This project implements an end-to-end **Delta Live Tables (DLT)** pipeline using the **Medallion Architecture** (Bronze → Silver → Gold). The pipeline processes streaming retail datasets (customers, products, sales, stores) and produces a full **star schema** consisting of dimension and fact tables.

---

## 🚀 **Project Overview**

The goal of this project is to build a production-ready streaming ETL pipeline using **Databricks Delta Live Tables** with:

* **cloudFiles Auto Loader ingestion**
* **Bronze, Silver, and Gold layers**
* **Auto-CDC‐driven SCD Type 1 & Type 2 tables**
* **Streaming views feeding streaming DLT tables**
* **Star schema modeling: `dim_customers`, `dim_products`, `dim_stores`, `fact_sales`**

---

## 🏗️ **Architecture (Medallion Pattern)**

**Bronze Layer**
Raw ingestion using Auto Loader:

* `customers_bronze`
* `products_bronze`
* `stores_bronze`
* `sales_bronze`

**Silver Layer**
Cleansed + enriched streaming tables (SCD Type 1 via Auto CDC):

* `customers_silver_view` → `customers_silver`
* `products_silver_view` → `products_silver`
* `stores_silver_view` → `stores_silver`
* `sales_silver_view` → `sales_silver`

Transformations include:

* Column normalization
* Derived attributes (`PricePerSale`, `domain`, etc.)
* Timestamps (`processDate`)
* Data quality / cleaning (string cleanup, type standardization)

**Gold Layer**
Star schema tables built with DLT Auto CDC:

* `customers_gold_view` → `dim_customers` (SCD Type 2)
* `products_gold_view` → `dim_products` (SCD Type 2)
* `stores_gold_view` → `dim_stores` (SCD Type 2)
* `sales_gold_view` → `fact_sales` (SCD Type 1)

---

## 📌 **Naming Conventions Used**

The project follows consistent and professional naming conventions:

### **Bronze Layer**

`<entity>_bronze`
Raw streaming tables (Auto Loader)

### **Silver Layer**

`<entity>_silver_view`
Enriched streaming view

`<entity>_silver`
SCD1 table created via `dlt.create_streaming_table`

### **Gold Layer**

`<entity>_gold_view`
Final cleaned streaming view feeding dimension/fact loads

`dim_<entity>`
SCD2 dimension tables (customers, products, stores)

`fact_sales`
SCD1 fact table

### **General Conventions**

* `processDate` used for CDC sequencing
* All incremental logic implemented via `dlt.create_auto_cdc_flow`
* Views generated with `@dlt.view`
* Tables generated with `@dlt.table` or `dlt.create_streaming_table`

---

## 📂 **Project Structure**

```
/Ingestion.py
/customers_silver.py
/products_silver.py
/stores_silver.py
/sales_silver.py
/dim_customers.py
/dim_products.py
/dim_stores.py
/fact_sales.py
```

---

## 🧩 **Technologies Used**

* Databricks Community Edition
* Delta Live Tables (DLT)
* Auto Loader (cloudFiles)
* PySpark / Spark Structured Streaming
* Delta Lake (SCD Type 1 & Type 2)
* Medallion Architecture (Bronze/Silver/Gold)



