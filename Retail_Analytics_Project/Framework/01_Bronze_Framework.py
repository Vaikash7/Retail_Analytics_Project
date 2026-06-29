# Databricks notebook source
# MAGIC %md
# MAGIC ####Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Metadata

# COMMAND ----------

config_df = (
    spark.table(
        "retail_catalog.metadata.source_config"
    )
    .filter(
        F.col("is_active") == "Y"
    )
)

schema_registry_df = spark.table(
    "retail_catalog.metadata.schema_registry"
)

display(config_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Bronze Framework

# COMMAND ----------

# ==========================================
# Bronze Framework Final
#
# Features
# ------------------------------------------
# Metadata Driven
# Auto Loader
# CSV / JSON / PARQUET
# Audit Columns
# Watermark Update
# Pipeline Logging
# Bronze Delta Tables
# ==========================================

for row in config_df.collect():

    try:

        # ==================================
        # Read Metadata
        # ==================================

        source_name = row["source_name"]
        source_path = row["source_path"]
        file_format = row["file_format"]
        target_table = row["target_table"]

        print(f"\nProcessing : {source_name}")

        # ==================================
        # Schema Location
        # ==================================

        schema_location = (
            f"/Volumes/retail_catalog/default/"
            f"retail_platform_volume/framework/schema/{source_name}"
        )

        checkpoint_path = (
            f"/Volumes/retail_catalog/default/"
            f"retail_platform_volume/framework/checkpoints/{source_name}"
        )

        # ==================================
        # Auto Loader Read
        # ==================================

        source_df = (
            spark.readStream
                 .format("cloudFiles")
                 .option(
                     "cloudFiles.format",
                     file_format
                 )
                 .option(
                     "cloudFiles.schemaLocation",
                     schema_location
                 )
                 .option(
                     "cloudFiles.schemaEvolutionMode",
                     "addNewColumns"
                 )
                 .option(
                     "rescuedDataColumn",
                     "_rescued_data"
                 )
                 .option(
                     "header",
                     "true"
                 )
                 .load(source_path)
        )

        # ==================================
        # Audit Columns
        # ==================================

        bronze_df = (
            source_df
                .withColumn(
                    "_AdfPipelineRunId",
                    F.lit("Manual_Run")
                )
                .withColumn(
                    "_IngestionTimestamp",
                    F.current_timestamp()
                )
        )

        # ==================================
        # Bronze Load
        # ==================================

        query = (
            bronze_df.writeStream
                     .format("delta")
                     .option(
                         "checkpointLocation",
                         checkpoint_path
                     )
                     .option(
                         "mergeSchema",
                         "true"
                     )
                     .trigger(
                         availableNow=True
                     )
                     .toTable(
                         f"retail_catalog.bronze.{target_table}"
                     )
        )

        query.awaitTermination()

        # ==================================
        # Watermark Update
        # ==================================

        spark.sql(f"""
        UPDATE retail_catalog.metadata.watermark
        SET last_processed_timestamp =
            current_timestamp()
        WHERE source_name =
            '{source_name}'
        """)

        # ==================================
        # Pipeline Success Log
        # ==================================

        spark.sql(f"""
        INSERT INTO retail_catalog.audit.pipeline_log
        (
            pipeline_name,
            source_name,
            status,
            records_processed,
            execution_time,
            error_message
        )
        VALUES
        (
            'Bronze_Framework',
            '{source_name}',
            'SUCCESS',
            0,
            current_timestamp(),
            NULL
        )
        """)

        print(
            f"{target_table} Loaded Successfully"
        )

    except Exception as e:

        error_message = (
            str(e)
            .replace("'", "")
            .replace("\n", " ")
        )

        spark.sql(f"""
        INSERT INTO retail_catalog.audit.pipeline_log
        (
            pipeline_name,
            source_name,
            status,
            records_processed,
            execution_time,
            error_message
        )
        VALUES
        (
            'Bronze_Framework',
            '{source_name}',
            'FAILED',
            0,
            current_timestamp(),
            '{error_message}'
        )
        """)

        print(
            f"{source_name} Failed"
        )

        print(error_message)

print(
    "\nBronze Framework Execution Completed"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN retail_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG retail_catalog;

# COMMAND ----------

customer_df = (
    spark.read
    .option("header", "true")
    .csv(
        "/Volumes/retail_catalog/default/retail_platform_volume/datasets/customers"
    )
)

print(
    "Customer Count :",
    customer_df.count()
)

display(customer_df)

# COMMAND ----------

print(
    f"Processing : {source_name}"
)

print(
    f"Source Path : {source_path}"
)

test_df = (
    spark.read
    .option("header","true")
    .csv(source_path)
)

print(
    f"Source Count : {test_df.count()}"
)

# COMMAND ----------

dbutils.notebook.exit(
    "SUCCESS"
)