# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

watermark_df = spark.table(
    "retail_catalog.metadata.watermark"
)

display(watermark_df)

# COMMAND ----------

watermark_row = (

    watermark_df

    .filter(
        F.col("source_name")
        == "customers"
    )

    .collect()[0]

)

watermark_value = watermark_row[
    "watermark_value"
]

print(
    watermark_value
)

# COMMAND ----------

customers_df = spark.table(
    "retail_catalog.silver.customers"
)

# COMMAND ----------

if watermark_value is None:

    incremental_df = customers_df

else:

    incremental_df = (

        customers_df

        .filter(
            F.col("LastUpdated")
            >
            F.lit(
                watermark_value
            )
        )

    )

# COMMAND ----------

print(
    "Incremental Records :",
    incremental_df.count()
)

# COMMAND ----------

max_watermark = (

    incremental_df

    .agg(

        F.max(
            "LastUpdated"
        )

    )

    .collect()[0][0]

)

print(
    max_watermark
)

# COMMAND ----------

if max_watermark is not None:

    spark.sql(f"""
    UPDATE retail_catalog.metadata.watermark
    SET watermark_value =
    TIMESTAMP('{max_watermark}')
    WHERE source_name='customers'
    """)

    print("Watermark Updated")

else:

    print(
        "No Incremental Records Found"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.metadata.watermark
# MAGIC WHERE source_name='customers'