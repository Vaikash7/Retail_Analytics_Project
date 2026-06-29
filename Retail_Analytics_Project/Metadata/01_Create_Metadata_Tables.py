# Databricks notebook source
# MAGIC %md
# MAGIC ####Using Pyspark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

# 1. Define schemas for metadata tables
source_config_schema = "source_name STRING, source_path STRING, file_format STRING, target_table STRING, load_type STRING, is_active STRING"
schema_registry_schema = "source_name STRING, column_name STRING, expected_datatype STRING"
quality_rules_schema = "table_name STRING, column_name STRING, rule_name STRING, rule_value STRING"
watermark_schema = "source_name STRING, last_processed_file STRING, last_processed_timestamp TIMESTAMP"

# 2. Create tables in the metadata schema
spark.sql("USE CATALOG retail_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")
spark.sql("CREATE SCHEMA IF NOT EXISTS audit")

# Create Metadata Tables
spark.sql(f"CREATE OR REPLACE TABLE metadata.source_config ({source_config_schema})")
spark.sql(f"CREATE OR REPLACE TABLE metadata.schema_registry ({schema_registry_schema})")
spark.sql(f"CREATE OR REPLACE TABLE metadata.quality_rules ({quality_rules_schema})")
spark.sql(f"CREATE OR REPLACE TABLE metadata.watermark ({watermark_schema})")

# 3. Create Audit and Log Tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_catalog.metadata.pipeline_audit
    (
        PipelineName STRING,
        LayerName STRING,
        StartTime TIMESTAMP,
        EndTime TIMESTAMP,
        Status STRING,
        RecordsProcessed BIGINT
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_catalog.metadata.error_log
    (
        PipelineName STRING,
        ErrorTime TIMESTAMP,
        ErrorMessage STRING
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS audit.pipeline_log
    (
        pipeline_name STRING,
        source_name STRING,
        status STRING,
        records_processed BIGINT,
        execution_time TIMESTAMP,
        error_message STRING
    ) USING DELTA
""")

# 4. Verification
print("Tables created successfully.")
spark.sql("SHOW TABLES IN retail_catalog.metadata").show()
spark.sql("SHOW TABLES IN audit").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_catalog;
# MAGIC
# MAGIC -- SOURCE CONFIG
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metadata.source_config
# MAGIC (
# MAGIC     source_name STRING,
# MAGIC     source_path STRING,
# MAGIC     file_format STRING,
# MAGIC     target_table STRING,
# MAGIC     load_type STRING,
# MAGIC     is_active STRING
# MAGIC );
# MAGIC
# MAGIC -- SCHEMA REGISTRY
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metadata.schema_registry
# MAGIC (
# MAGIC     source_name STRING,
# MAGIC     column_name STRING,
# MAGIC     expected_datatype STRING
# MAGIC );
# MAGIC
# MAGIC -- QUALITY RULES
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metadata.quality_rules
# MAGIC (
# MAGIC     table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     rule_name STRING,
# MAGIC     rule_value STRING
# MAGIC );
# MAGIC
# MAGIC -- WATERMARK
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metadata.watermark
# MAGIC (
# MAGIC     source_name STRING,
# MAGIC     last_processed_file STRING,
# MAGIC     last_processed_timestamp TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC -- PIPELINE LOG
# MAGIC
# MAGIC CREATE OR REPLACE TABLE audit.pipeline_log
# MAGIC (
# MAGIC     pipeline_name STRING,
# MAGIC     source_name STRING,
# MAGIC     status STRING,
# MAGIC     records_processed BIGINT,
# MAGIC     execution_time TIMESTAMP,
# MAGIC     error_message STRING
# MAGIC );
# MAGIC
# MAGIC SHOW TABLES IN metadata;
# MAGIC
# MAGIC SHOW TABLES IN audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC retail_catalog.metadata.pipeline_audit
# MAGIC (
# MAGIC     PipelineName STRING,
# MAGIC     LayerName STRING,
# MAGIC     StartTime TIMESTAMP,
# MAGIC     EndTime TIMESTAMP,
# MAGIC     Status STRING,
# MAGIC     RecordsProcessed BIGINT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC retail_catalog.metadata.error_log
# MAGIC (
# MAGIC     PipelineName STRING,
# MAGIC     ErrorTime TIMESTAMP,
# MAGIC     ErrorMessage STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_catalog.metadata;