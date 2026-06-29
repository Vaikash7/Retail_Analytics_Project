# Databricks notebook source
# MAGIC %md
# MAGIC ####MASTER FRAMEWORK

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

start_time = datetime.now()

print(
    f"Pipeline Started : {start_time}"
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/01_Bronze_Framework",
    0
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/02_Silver_Framework",
    0
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/06_Watermark_Framework",
    0
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/03_Gold_Initial_Load",
    0
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/04_Gold_Framework",
    0
)

# COMMAND ----------

dbutils.notebook.run(
    "/Users/vaikashch@gmail.com/Retail_Analytics_Project/Framework/05_SCD2_Customer_Dimension",
    0
)

# COMMAND ----------

end_time = datetime.now()

print(
    f"Pipeline Ended : {end_time}"
)

# COMMAND ----------

duration = end_time - start_time

print(
    f"Execution Time : {duration}"
)

# COMMAND ----------

spark.sql(f"""
INSERT INTO retail_catalog.metadata.pipeline_audit
VALUES
(
'Master_Framework',
'Full_Pipeline',
TIMESTAMP('{start_time}'),
TIMESTAMP('{end_time}'),
'SUCCESS',
0
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM retail_catalog.metadata.pipeline_audit
# MAGIC ORDER BY StartTime DESC