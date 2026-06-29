# Databricks notebook source
print("Hello")
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.bronze.customers;

# COMMAND ----------

df = spark.read.csv("/Volumes/retail_catalog/default/retail_platform_volume/datasets/customers/customers.csv",header=True)

df.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_customers_count
# MAGIC FROM retail_catalog.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_customers_count
# MAGIC FROM retail_catalog.silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_catalog.reject.orders;