# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM retail_catalog.gold.dim_customers
# MAGIC WHERE CustomerID = 2000

# COMMAND ----------

customer_updates_df = (

    spark.table(
        "retail_catalog.silver.customers"
    )

    .withColumn(

        "City",

        when(
            col("CustomerID") == 2000,
            "Mumbai"
        )

        .otherwise(
            col("City")
        )

    )

)

customer_updates_df.createOrReplaceTempView(
    "customer_updates"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM customer_updates
# MAGIC WHERE CustomerID = 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO retail_catalog.gold.dim_customers t
# MAGIC
# MAGIC USING customer_updates s
# MAGIC
# MAGIC ON t.CustomerID = s.CustomerID
# MAGIC AND t.IsCurrent = true
# MAGIC
# MAGIC WHEN MATCHED
# MAGIC
# MAGIC AND
# MAGIC (
# MAGIC     t.City <> s.City
# MAGIC     OR
# MAGIC     t.State <> s.State
# MAGIC )
# MAGIC
# MAGIC THEN UPDATE SET
# MAGIC
# MAGIC t.EffectiveEndDate = current_timestamp(),
# MAGIC
# MAGIC t.IsCurrent = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC CustomerID,
# MAGIC City,
# MAGIC State,
# MAGIC EffectiveStartDate,
# MAGIC EffectiveEndDate,
# MAGIC IsCurrent
# MAGIC FROM retail_catalog.gold.dim_customers
# MAGIC WHERE CustomerID = 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO retail_catalog.gold.dim_customers
# MAGIC
# MAGIC SELECT
# MAGIC
# MAGIC s.CustomerID,
# MAGIC s.FirstName,
# MAGIC s.LastName,
# MAGIC s.Email,
# MAGIC s.Phone,
# MAGIC s.City,
# MAGIC s.State,
# MAGIC
# MAGIC current_timestamp() as EffectiveStartDate,
# MAGIC
# MAGIC CAST(NULL AS TIMESTAMP) as EffectiveEndDate,
# MAGIC
# MAGIC true as IsCurrent
# MAGIC
# MAGIC FROM customer_updates s
# MAGIC
# MAGIC LEFT JOIN retail_catalog.gold.dim_customers t
# MAGIC
# MAGIC ON s.CustomerID = t.CustomerID
# MAGIC
# MAGIC AND t.IsCurrent = true
# MAGIC
# MAGIC WHERE
# MAGIC
# MAGIC t.CustomerID IS NULL
# MAGIC
# MAGIC OR
# MAGIC
# MAGIC t.City <> s.City
# MAGIC
# MAGIC OR
# MAGIC
# MAGIC t.State <> s.State

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC
# MAGIC CustomerID,
# MAGIC City,
# MAGIC State,
# MAGIC EffectiveStartDate,
# MAGIC EffectiveEndDate,
# MAGIC IsCurrent
# MAGIC
# MAGIC FROM retail_catalog.gold.dim_customers
# MAGIC
# MAGIC WHERE CustomerID = 2000
# MAGIC
# MAGIC ORDER BY EffectiveStartDate