# Databricks notebook source
# MAGIC %md
# MAGIC ####Verify Bronze Record Count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_catalog.bronze.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Silver Record Count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_catalog.silver.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Gold Record Count

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_catalog.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Duplicate Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OrderID, COUNT(*)
# MAGIC FROM retail_catalog.gold.fact_orders
# MAGIC GROUP BY OrderID
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Null Values

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.gold.fact_orders
# MAGIC WHERE OrderDate IS NULL
# MAGIC OR Category IS NULL
# MAGIC OR Brand IS NULL
# MAGIC OR City IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Sales Amount
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC Quantity,
# MAGIC UnitPrice,
# MAGIC SalesAmount
# MAGIC FROM retail_catalog.gold.fact_orders
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Fact Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail_catalog.gold.fact_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verify Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail_catalog.gold.dim_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM retail_catalog.gold.dim_products;

# COMMAND ----------

print("=" * 70)
print("ENTERPRISE RETAIL ANALYTICS PLATFORM - UNIT TESTING")
print("=" * 70)

# =============================================
# Test 1 - Bronze Orders
# =============================================
bronze_orders = spark.table("retail_catalog.bronze.orders").count()
assert bronze_orders > 0
print(f"✅ Test 1 Passed - Bronze Orders : {bronze_orders}")

# =============================================
# Test 2 - Bronze Products
# =============================================
bronze_products = spark.table("retail_catalog.bronze.products").count()
assert bronze_products > 0
print(f"✅ Test 2 Passed - Bronze Products : {bronze_products}")

# =============================================
# Test 3 - Bronze Customers
# =============================================
bronze_customers = spark.table("retail_catalog.bronze.customers").count()
assert bronze_customers > 0
print(f"✅ Test 3 Passed - Bronze Customers : {bronze_customers}")

# =============================================
# Test 4 - Bronze Exchange Rates
# =============================================
bronze_exchange = spark.table("retail_catalog.bronze.exchange_rates").count()
assert bronze_exchange > 0
print(f"✅ Test 4 Passed - Bronze Exchange Rates : {bronze_exchange}")

# =============================================
# Test 5 - Silver Orders
# =============================================
silver_orders = spark.table("retail_catalog.silver.orders").count()
assert silver_orders > 0
print(f"✅ Test 5 Passed - Silver Orders : {silver_orders}")

# =============================================
# Test 6 - Silver Products
# =============================================
silver_products = spark.table("retail_catalog.silver.products").count()
assert silver_products > 0
print(f"✅ Test 6 Passed - Silver Products : {silver_products}")

# =============================================
# Test 7 - Silver Customers
# =============================================
silver_customers = spark.table("retail_catalog.silver.customers").count()
assert silver_customers > 0
print(f"✅ Test 7 Passed - Silver Customers : {silver_customers}")

# =============================================
# Test 8 - Silver Exchange Rates
# =============================================
silver_exchange = spark.table("retail_catalog.silver.exchange_rates").count()
assert silver_exchange > 0
print(f"✅ Test 8 Passed - Silver Exchange Rates : {silver_exchange}")

# =============================================
# Test 9 - Gold Fact Orders
# =============================================
fact_orders = spark.table("retail_catalog.gold.fact_orders").count()
assert fact_orders > 0
print(f"✅ Test 9 Passed - Fact Orders : {fact_orders}")

# =============================================
# Test 10 - Gold Dim Customers
# =============================================
dim_customers = spark.table("retail_catalog.gold.dim_customers").count()
assert dim_customers > 0
print(f"✅ Test 10 Passed - Dim Customers : {dim_customers}")

# =============================================
# Test 11 - Gold Dim Products
# =============================================
dim_products = spark.table("retail_catalog.gold.dim_products").count()
assert dim_products > 0
print(f"✅ Test 11 Passed - Dim Products : {dim_products}")

# =============================================
# Test 12 - Null Values Validation
# =============================================
null_count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM retail_catalog.gold.fact_orders
WHERE OrderDate IS NULL
   OR Category IS NULL
   OR SubCategory IS NULL
   OR Brand IS NULL
   OR City IS NULL
   OR State IS NULL
""").collect()[0]["cnt"]

assert null_count == 0
print(f"✅ Test 12 Passed - Null Values : {null_count}")

# =============================================
# Test 13 - Duplicate Orders
# =============================================
duplicate_orders = spark.sql("""
SELECT COUNT(*) cnt
FROM
(
    SELECT OrderID
    FROM retail_catalog.gold.fact_orders
    GROUP BY OrderID
    HAVING COUNT(*) > 1
)
""").collect()[0]["cnt"]

assert duplicate_orders == 0
print(f"✅ Test 13 Passed - Duplicate Orders : {duplicate_orders}")

# =============================================
# Test 14 - Negative Sales Amount
# =============================================
negative_sales = spark.sql("""
SELECT COUNT(*) cnt
FROM retail_catalog.gold.fact_orders
WHERE SalesAmount <= 0
""").collect()[0]["cnt"]

assert negative_sales == 0
print(f"✅ Test 14 Passed - Negative Sales : {negative_sales}")

print("=" * 70)
print("🎉 ALL UNIT TESTS EXECUTED SUCCESSFULLY")
print("=" * 70)