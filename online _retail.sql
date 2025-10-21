-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import (
-- MAGIC     col, trim, upper, regexp_replace, when, to_timestamp
-- MAGIC )
-- MAGIC
-- MAGIC # 1️⃣ Initialize Spark session
-- MAGIC spark = SparkSession.builder.appName("OnlineRetail_Cleaning").getOrCreate()
-- MAGIC
-- MAGIC # 2️⃣ Read data directly from existing Databricks table
-- MAGIC df = spark.table("workspace.default.online_retail_ii")
-- MAGIC
-- MAGIC print("✅ Data loaded successfully from workspace.default.online_retail_ii")
-- MAGIC df.show(5)
-- MAGIC
-- MAGIC # 3️⃣ Clean data
-- MAGIC df_clean = (
-- MAGIC     df.withColumn("StockCode", upper(trim(regexp_replace(col("StockCode"), "[^A-Za-z0-9]", ""))))
-- MAGIC       .withColumn("Description", trim(col("Description")))
-- MAGIC       .withColumn("CustomerID", when(col("Customer ID").rlike("^[0-9]+$"), col("Customer ID").cast("bigint")).otherwise(None))
-- MAGIC       .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))
-- MAGIC       .withColumn("TotalAmount", col("Quantity") * col("Price"))
-- MAGIC       .drop("Customer ID")
-- MAGIC )
-- MAGIC
-- MAGIC print("✅ Data cleaned successfully")
-- MAGIC df_clean.printSchema()
-- MAGIC df_clean.show(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_clean.write.format("delta").mode("overwrite").saveAsTable("default.online_retail_cleaned")
-- MAGIC
-- MAGIC print("✅ Table saved successfully as: default.online_retail_cleaned")
-- MAGIC
-- MAGIC # 5️⃣ Verify tables inside 'default' schema
-- MAGIC print("📊 Showing tables in default schema:")
-- MAGIC spark.sql("SHOW TABLES IN default").show()

-- COMMAND ----------


SELECT * FROM default.online_retail_cleaned LIMIT 10;