# Databricks notebook source
# MAGIC %md
# MAGIC ####Silver Framework

# COMMAND ----------

from pyspark.sql import functions as F

config_df = spark.table(
    "retail_catalog.metadata.source_config"
)

schema_df = spark.table(
    "retail_catalog.metadata.schema_registry"
)

rules_df = spark.table(
    "retail_catalog.metadata.quality_rules"
)

watermark_df = spark.table(
    "retail_catalog.metadata.watermark"
)

# COMMAND ----------

# ==========================================
# Silver Framework
#
# Features
# ------------------------------------------
# Metadata Driven
# Quality Rules
# Reject Handling
# Type Casting
# Audit Columns
# Silver Tables
# Reject Tables
# ==========================================

for row in config_df.collect():

    # ======================================
    # Read Metadata
    # ======================================

    source_name = row["source_name"]

    print(f"\nProcessing : {source_name}")

    # ======================================
    # Read Bronze Table
    # ======================================

    bronze_df = spark.table(
        f"retail_catalog.bronze.{source_name}"
    )

    working_df = bronze_df

    # ======================================
    # Type Casting as per Document
    # ======================================

    if source_name == "orders":

        working_df = (
            working_df
            .withColumn(
                "OrderID",
                F.expr(
                    "try_cast(OrderID as bigint)"
                )
            )
            .withColumn(
                "CustomerID",
                F.expr(
                    "try_cast(CustomerID as int)"
                )
            )
            .withColumn(
                "ProductID",
                F.expr(
                    "try_cast(ProductID as int)"
                )
            )
            .withColumn(
                "OrderDate",
                F.expr(
                    "try_cast(OrderDate as date)"
                )
            )
            .withColumn(
                "Quantity",
                F.expr(
                    "try_cast(Quantity as int)"
                )
            )
            .withColumn(
                "UnitPrice",
                F.expr(
                    "try_cast(UnitPrice as decimal(10,2))"
                )
            )
        )

    elif source_name == "products":

        working_df = (
            working_df
            .withColumn(
                "ProductID",
                F.expr(
                    "try_cast(ProductID as int)"
                )
            )
            .withColumn(
                "CostPrice",
                F.expr(
                    "try_cast(CostPrice as decimal(10,2))"
                )
            )
        )

    elif source_name == "customers":

        working_df = (
            working_df
            .withColumn(
                "CustomerID",
                F.expr(
                    "try_cast(CustomerID as int)"
                )
            )
            .withColumn(
                "LastUpdated",
                F.expr(
                    "try_cast(LastUpdated as timestamp)"
                )
            )
        )

    elif source_name == "exchange_rates":

        rates_schema = StructType([

            StructField(
                "AED",
                StringType(),
                True
            ),

            StructField(
                "EUR",
                StringType(),
                True
            ),

            StructField(
                "GBP",
                StringType(),
                True
            ),

            StructField(
                "INR",
                StringType(),
                True
            ),

            StructField(
                "JPY",
                StringType(),
                True
            )

        ])

        working_df = (
            working_df
            .withColumn(
                "rates",
                F.from_json(
                    F.col("rates"),
                    rates_schema
                )
            )
        )

        working_df = flatten_json(
            working_df
        )

        working_df = (
            working_df
            .withColumnRenamed(
                "base",
                "BaseCurrency"
            )
            .withColumnRenamed(
                "date",
                "RateDate"
            )
        )

    # ======================================
    # Deduplication Logic
    # ======================================

    if source_name == "customers":

        from pyspark.sql.window import Window

        window_spec = (
            Window
            .partitionBy("CustomerID")
            .orderBy(
                F.col("LastUpdated").desc()
            )
        )

        working_df = (
            working_df
            .withColumn(
                "rn",
                F.row_number().over(
                    window_spec
                )
            )
            .filter(
                F.col("rn") == 1
            )
            .drop("rn")
        )

        print(
            "Customer Deduplication Completed"
        )

    elif source_name == "products":

        working_df = (
            working_df
            .dropDuplicates(
                ["ProductID"]
            )
        )

        print(
            "Product Deduplication Completed"
        )

    elif source_name == "orders":

        working_df = (
            working_df
            .dropDuplicates(
                ["OrderID"]
            )
        )

        print(
            "Order Deduplication Completed"
        )

    elif source_name == "exchange_rates":

        working_df = (
            working_df
            .dropDuplicates(
                [
                    "BaseCurrency",
                    "RateDate"
                ]
            )
        )

        print(
            "Exchange Rate Deduplication Completed"
        )

    # ======================================
    # NEW: Mandatory Pre-Processing Filters
    # ======================================
    if source_name == "orders":
        working_df = working_df.filter(
            F.col("OrderID").isNotNull() & 
            F.col("CustomerID").isNotNull() & 
            F.col("ProductID").isNotNull() & 
            F.col("OrderDate").isNotNull()
        )
    elif source_name == "products":
        working_df = working_df.filter(
            F.col("Category").isNotNull() & 
            F.col("SubCategory").isNotNull() & 
            F.col("Brand").isNotNull()
        )
    elif source_name == "customers":
        working_df = working_df.filter(
            F.col("City").isNotNull() & 
            F.col("State").isNotNull()
        )

    # ======================================
    # Read Quality Rules
    # ======================================

    reject_condition = None
    reject_reason = None

    source_rules = (
        rules_df
        .filter(
            F.col("table_name")
            == source_name
        )
        .collect()
    )

    # ======================================
    # Apply Quality Rules
    # ======================================

    for rule in source_rules:

        column_name = rule["column_name"]
        rule_name = rule["rule_name"]
        rule_value = rule["rule_value"]

        # ----------------------------------
        # Greater Than Validation
        # ----------------------------------

        if rule_name == "greater_than":

            condition = (
                F.col(column_name)
                <= float(rule_value)
            )

            reason_expr = F.when(
                condition,
                F.lit(
                    f"{column_name} <= {rule_value}"
                )
            )

        # ----------------------------------
        # Not Null Validation
        # ----------------------------------

        elif rule_name == "not_null":

            condition = (
                F.col(column_name)
                .isNull()
            )

            reason_expr = F.when(
                condition,
                F.lit(
                    f"{column_name} is NULL"
                )
            )

        # ----------------------------------
        # Valid Date Validation
        # ----------------------------------

        elif rule_name == "valid_date":

            condition = (
                F.col(column_name)
                .isNull()
            )

            reason_expr = F.when(
                condition,
                F.lit(
                    f"{column_name} Invalid Date"
                )
            )

        else:

            continue

        # ----------------------------------
        # Build Reject Condition
        # ----------------------------------

        if reject_condition is None:

            reject_condition = condition

        else:

            reject_condition = (
                reject_condition
                | condition
            )

        # ----------------------------------
        # Build Reject Reason
        # ----------------------------------

        if reject_reason is None:

            reject_reason = reason_expr

        else:

            reject_reason = F.coalesce(
                reject_reason,
                reason_expr
            )

    # ======================================
    # Split Valid / Reject Records
    # ======================================

    if reject_condition is not None:

        reject_df = (
            working_df
            .filter(
                reject_condition
            )
        )

        valid_df = (
            working_df
            .filter(
                ~reject_condition
            )
        )

    else:

        valid_df = working_df

        reject_df = (
            spark.createDataFrame(
                [],
                working_df.schema
            )
        )

    # ======================================
    # Silver Audit Columns
    # ======================================

    valid_df = (
        valid_df
        .withColumn(
            "_ProcessedTimestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_IsRejected",
            F.lit(False)
        )
    )

    # ======================================
    # ======================================
    # Reject Audit Columns
    # ======================================

    reject_df = (
    reject_df
    .withColumn(
        "_ProcessedTimestamp",
        F.current_timestamp()
    )
    .withColumn(
        "_IsRejected",
        F.lit(True)
    )
    .withColumn(
        "RejectReason",
        F.coalesce(
            reject_reason,
            F.lit("Unknown")
        )
        if reject_reason is not None
        else F.lit("No Rule Defined")
    )
)

    # ======================================
    # Write Silver Tables
    # ======================================

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"retail_catalog.silver.{source_name}"
        )
    )

    # ======================================
    # Write Reject Tables
    # ======================================

    (
        reject_df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"retail_catalog.reject.{source_name}"
        )
    )

    print(
        f"{source_name} Silver Load Completed"
    )

# ==========================================
# Framework Completion
# ==========================================

print(
    "\nSilver Framework Execution Completed"
)

# COMMAND ----------

print("Cell 2 completed")

# COMMAND ----------

valid_count = valid_df.count()

reject_count = reject_df.count()

total_count = (
    valid_count +
    reject_count
)

print(
    f"Total Records  : {total_count}"
)

print(
    f"Valid Records  : {valid_count}"
)

print(
    f"Reject Records : {reject_count}"
)

# COMMAND ----------

print("Cell 3 completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.reject.orders;

# COMMAND ----------

print("Cell 4 completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE retail_catalog.silver.products;

# COMMAND ----------

print("Cell 5 completed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### JSON Detection

# COMMAND ----------

# ======================================
# Auto Detect Complex JSON
# ======================================

has_complex_columns = any(

    isinstance(
        field.dataType,
        (StructType, ArrayType)
    )

    for field in working_df.schema.fields
)

if has_complex_columns:

    print(
        f"Flattening JSON : {source_name}"
    )

    working_df = flatten_json(
        working_df
    )

# COMMAND ----------

print("Cell 7 completed")

# COMMAND ----------

spark.table(
    "retail_catalog.silver.exchange_rates"
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Auto Flatten

# COMMAND ----------

# ======================================
# Auto JSON Flattening
# ======================================

def flatten_json(df):

    complex_fields = True

    while complex_fields:

        complex_fields = False

        for field in df.schema.fields:

            field_name = field.name
            field_type = field.dataType

            # Struct Handling

            if isinstance(
                field_type,
                StructType
            ):

                expanded_cols = [

                    F.col(
                        f"{field_name}.{nested.name}"
                    ).alias(
                        f"{field_name}_{nested.name}"
                    )

                    for nested in field_type.fields

                ]

                df = (
                    df.select(
                        "*",
                        *expanded_cols
                    )
                    .drop(field_name)
                )

                complex_fields = True

                break

            # Array Handling

            elif isinstance(
                field_type,
                ArrayType
            ):

                df = (
                    df.withColumn(
                        field_name,
                        F.explode_outer(
                            F.col(field_name)
                        )
                    )
                )

                complex_fields = True

                break

    return df

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

print("Cell 8 completed")

# COMMAND ----------

# ==========================================
# Imports
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

# ==========================================
# Read Metadata
# ==========================================

config_df = (
    spark.table(
        "retail_catalog.metadata.source_config"
    )
    .filter(
        F.col("is_active") == "Y"
    )
)

rules_df = spark.table(
    "retail_catalog.metadata.quality_rules"
)

display(config_df)
display(rules_df)

# COMMAND ----------

print("Cell 9 completed")

# COMMAND ----------

print(
    spark.table(
        "retail_catalog.bronze.customers"
    ).count()
)

# COMMAND ----------

print("Cell 10 completed")

# COMMAND ----------

display(
    spark.table(
        "retail_catalog.silver.customers"
    )
)

# COMMAND ----------

print("Cell 11 completed")

# COMMAND ----------

print(
    "\nSilver Framework Execution Completed"
)

dbutils.notebook.exit(
    "SUCCESS"
)

# COMMAND ----------

print("Cell 12 completed")