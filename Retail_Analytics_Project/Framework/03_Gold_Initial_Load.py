# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.table(
    "retail_catalog.silver.customers"
)

display(silver_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_catalog.gold.dim_customers
# MAGIC (
# MAGIC     CustomerID INT,
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     Email STRING,
# MAGIC     Phone STRING,
# MAGIC     City STRING,
# MAGIC     State STRING,
# MAGIC
# MAGIC     EffectiveStartDate TIMESTAMP,
# MAGIC     EffectiveEndDate TIMESTAMP,
# MAGIC
# MAGIC     IsCurrent BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

initial_df = (

    silver_df

    .select(

        "CustomerID",
        "FirstName",
        "LastName",
        "Email",
        "Phone",
        "City",
        "State"

    )

    .withColumn(
        "EffectiveStartDate",
        F.current_timestamp()
    )

    .withColumn(
        "EffectiveEndDate",
        F.lit(None).cast("timestamp")
    )

    .withColumn(
        "IsCurrent",
        F.lit(True)
    )

)

# COMMAND ----------

(
    initial_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "retail_catalog.gold.dim_customers"
    )
)

# COMMAND ----------

print(
    "Gold Customer Count :",
    spark.table(
        "retail_catalog.gold.dim_customers"
    ).count()
)

# COMMAND ----------

display(
    spark.table(
        "retail_catalog.gold.dim_customers"
    )
)

# COMMAND ----------

display(
    spark.table(
        "retail_catalog.gold.dim_customers"
    )
    .filter(
        F.col("IsCurrent") == True
    )
)

# COMMAND ----------

spark.table(
    "retail_catalog.gold.dim_customers"
).printSchema()