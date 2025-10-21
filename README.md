The Dataset :- https://archive.ics.uci.edu/dataset/502/online+retail+ii

# 🧠 Online Retail ETL with PySpark & Delta Lake

## 📌 Project Overview
This project demonstrates how to build an **ETL pipeline** (Extract, Transform, Load) using **PySpark** within **Databricks**.  
We process a real-world **Online Retail dataset**, clean and validate the data, and store it in a **Delta Lake** table for analytics.

The goal is to showcase practical **data engineering** skills using:
- Apache Spark (PySpark)
- Databricks Workspace
- Delta Lake Tables
- Spark SQL

---

## 🧩 Tech Stack
- **Language:** Python (PySpark)
- **Platform:** Databricks (Community Edition)
- **Storage Format:** Delta Lake
- **Tools:** Spark SQL, DataFrames, Delta Engine

---

## 📊 Dataset
**Dataset Used:** Online Retail Dataset  
**Columns:**
- `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`

Data Source: UCI Machine Learning Repository / Kaggle (Public)

---

## ⚙️ ETL Workflow

### **1️⃣ Extract**
Load the dataset from Databricks FileStore (or S3 bucket if external).

```python
df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/online_retail.csv")
df.show(5)
