# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

orders_df = spark.table(
    "retail_catalog.silver.orders"
)

products_df = spark.table(
    "retail_catalog.silver.products"
)

customers_df = spark.table(
    "retail_catalog.silver.customers"
)

exchange_rates_df = spark.table(
    "retail_catalog.silver.exchange_rates"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DIM_PRODUCTS

# COMMAND ----------

dim_products_df = (

    products_df

    .select(
        "ProductID",
        "ProductName",
        "Category",
        "SubCategory",
        "Brand",
        "CostPrice"
    )

    .dropDuplicates(
        ["ProductID"]
    )

)

# COMMAND ----------

(
    dim_products_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "retail_catalog.gold.dim_products"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####FACT_ORDERS

# COMMAND ----------

fact_orders_df = (
    orders_df.alias("o")
    .join(
        products_df.alias("p"),
        F.col("o.ProductID") == F.col("p.ProductID"),
        "inner"  
    )
    .join(
        customers_df.alias("c"),
        F.col("o.CustomerID") == F.col("c.CustomerID"),
        "inner"  
    )
    .select(
        F.col("o.OrderID"),
        F.col("o.CustomerID"),
        F.col("o.ProductID"),
        F.col("o.OrderDate"),
        F.col("o.StoreCode"),
        F.col("o.Quantity"),
        F.col("o.UnitPrice"),
        F.col("p.Category"),
        F.col("p.SubCategory"),
        F.col("p.Brand"),
        F.col("c.City"),
        F.col("c.State")
    )
    .withColumn(
        "SalesAmount",
        F.round(
            F.col("Quantity") * F.col("UnitPrice"),
            2
        )
    )
    .dropna(
        subset=[
            "OrderDate",
            "Category",
            "SubCategory",
            "Brand",
            "City",
            "State"
        ]
    )
)

# COMMAND ----------

(
    fact_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "retail_catalog.gold.fact_orders"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####DAILY SALES SUMMARY

# COMMAND ----------

daily_sales_df = (

    fact_orders_df

    .groupBy(
        "OrderDate"
    )

    .agg(

        F.countDistinct(
            "OrderID"
        ).alias(
            "TotalOrders"
        ),

        F.round(

            F.sum(
                "SalesAmount"
            ),

            2

        ).alias(
            "TotalRevenue"
        )

    )

)

# COMMAND ----------

(
    daily_sales_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "retail_catalog.gold.daily_sales_summary"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####CATEGORY PERFORMANCE
# MAGIC

# COMMAND ----------

category_perf_df = (

    fact_orders_df

    .groupBy(
        "Category"
    )

    .agg(

        F.sum(
            "Quantity"
        ).alias(
            "TotalQuantity"
        ),

        F.round(

            F.sum(
                "SalesAmount"
            ),

            2

        ).alias(
            "TotalRevenue"
        )

    )

)

# COMMAND ----------

(
    category_perf_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "retail_catalog.gold.category_performance"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####CITY REVENUE SUMMARY

# COMMAND ----------

city_revenue_df = (

    fact_orders_df

    .groupBy(
        "City",
        "State"
    )

    .agg(

        F.countDistinct(
            "OrderID"
        ).alias(
            "TotalOrders"
        ),

        F.round(

            F.sum(
                "SalesAmount"
            ),

            2

        ).alias(
            "Revenue"
        )

    )

)

# COMMAND ----------

(
    city_revenue_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "retail_catalog.gold.city_revenue_summary"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN retail_catalog.gold;