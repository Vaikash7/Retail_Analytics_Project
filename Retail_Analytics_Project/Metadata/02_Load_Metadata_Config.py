# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG retail_catalog;
# MAGIC
# MAGIC -- =========================================
# MAGIC -- SOURCE CONFIG
# MAGIC -- =========================================
# MAGIC
# MAGIC INSERT INTO metadata.source_config
# MAGIC VALUES
# MAGIC (
# MAGIC 'orders',
# MAGIC '/Volumes/retail_catalog/default/retail_platform_volume/datasets/orders',
# MAGIC 'csv',
# MAGIC 'orders',
# MAGIC 'full',
# MAGIC 'Y'
# MAGIC ),
# MAGIC (
# MAGIC 'products',
# MAGIC '/Volumes/retail_catalog/default/retail_platform_volume/datasets/products',
# MAGIC 'csv',
# MAGIC 'products',
# MAGIC 'full',
# MAGIC 'Y'
# MAGIC ),
# MAGIC (
# MAGIC 'customers',
# MAGIC '/Volumes/retail_catalog/default/retail_platform_volume/datasets/customers',
# MAGIC 'csv',
# MAGIC 'customers',
# MAGIC 'full',
# MAGIC 'Y'
# MAGIC ),
# MAGIC (
# MAGIC 'exchange_rates',
# MAGIC '/Volumes/retail_catalog/default/retail_platform_volume/datasets/exchange_rates',
# MAGIC 'json',
# MAGIC 'exchange_rates',
# MAGIC 'full',
# MAGIC 'Y'
# MAGIC );
# MAGIC
# MAGIC -- =========================================
# MAGIC -- SCHEMA REGISTRY
# MAGIC -- =========================================
# MAGIC
# MAGIC INSERT INTO metadata.schema_registry
# MAGIC VALUES
# MAGIC
# MAGIC -- ORDERS
# MAGIC
# MAGIC ('orders','OrderID','string'),
# MAGIC ('orders','CustomerID','string'),
# MAGIC ('orders','ProductID','string'),
# MAGIC ('orders','OrderDate','string'),
# MAGIC ('orders','Quantity','string'),
# MAGIC ('orders','UnitPrice','string'),
# MAGIC ('orders','StoreCode','string'),
# MAGIC
# MAGIC -- PRODUCTS
# MAGIC
# MAGIC ('products','ProductID','string'),
# MAGIC ('products','ProductName','string'),
# MAGIC ('products','Category','string'),
# MAGIC ('products','SubCategory','string'),
# MAGIC ('products','Brand','string'),
# MAGIC ('products','CostPrice','string'),
# MAGIC
# MAGIC -- CUSTOMERS
# MAGIC
# MAGIC ('customers','CustomerID','string'),
# MAGIC ('customers','FirstName','string'),
# MAGIC ('customers','LastName','string'),
# MAGIC ('customers','Email','string'),
# MAGIC ('customers','Phone','string'),
# MAGIC ('customers','City','string'),
# MAGIC ('customers','State','string'),
# MAGIC ('customers','LastUpdated','string'),
# MAGIC
# MAGIC -- EXCHANGE RATES
# MAGIC
# MAGIC ('exchange_rates','base','string'),
# MAGIC ('exchange_rates','date','string');
# MAGIC
# MAGIC -- =========================================
# MAGIC -- QUALITY RULES
# MAGIC -- =========================================
# MAGIC
# MAGIC INSERT INTO metadata.quality_rules
# MAGIC VALUES
# MAGIC
# MAGIC ('orders','Quantity','greater_than','0'),
# MAGIC ('orders','UnitPrice','greater_than','0'),
# MAGIC
# MAGIC ('products','CostPrice','greater_than','0'),
# MAGIC
# MAGIC ('customers','Email','not_null','Y'),
# MAGIC
# MAGIC ('customers','Phone','valid_phone','Y');
# MAGIC
# MAGIC -- =========================================
# MAGIC -- WATERMARK
# MAGIC -- =========================================
# MAGIC
# MAGIC INSERT INTO metadata.watermark
# MAGIC VALUES
# MAGIC
# MAGIC ('orders',NULL,NULL),
# MAGIC ('products',NULL,NULL),
# MAGIC ('customers',NULL,NULL),
# MAGIC ('exchange_rates',NULL,NULL);
# MAGIC
# MAGIC -- =========================================
# MAGIC -- VALIDATION
# MAGIC -- =========================================
# MAGIC
# MAGIC SELECT * FROM metadata.source_config;
# MAGIC
# MAGIC SELECT * FROM metadata.schema_registry;
# MAGIC
# MAGIC SELECT * FROM metadata.quality_rules;
# MAGIC
# MAGIC SELECT * FROM metadata.watermark;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM retail_catalog.reject.orders;