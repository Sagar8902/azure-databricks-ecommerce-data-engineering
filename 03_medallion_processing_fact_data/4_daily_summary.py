# Databricks notebook source
# MAGIC %md
# MAGIC ### Daily Summary Table

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

days_cutoff = 30
source_table_name = "gld_fact_order_items"
table_name = "gld_fact_daily_orders_summary"

# COMMAND ----------

max_date_row = spark.sql(f"""
    SELECT MAX(transaction_date) AS max_date 
    FROM {catalog_name}.gold.{source_table_name}
""").collect()[0]

max_date = max_date_row['max_date']
print(max_date)

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.gold.{table_name}"):
    where_clause = f"transaction_date >= date_sub(date('{max_date}'), {days_cutoff})" # max_date
else: 
    where_clause = "1=1"

# COMMAND ----------

summary_query = f"""
SELECT
date_id,
unit_price_currency as currency,
SUM(quantity) as total_quantity,
SUM(gross_amount) as total_gross_amount,
SUM(discount_amount) as total_discount_amount,
SUM(tax_amount) as total_tax_amount,
SUM(net_amount) as total_amount
FROM
{catalog_name}.gold.{source_table_name}
WHERE {where_clause}
GROUP BY date_id, currency
Order By date_id Desc
"""
summary_df = spark.sql(summary_query)

# COMMAND ----------

# This code maintains a daily summary Delta table.
# - On the first run, it creates the table with all historical data.
# - On later runs, it recalculates the last N days (e.g., 30), then merges: updating existing dates and inserting new ones to keep the summary accurate.

if not spark.catalog.tableExists(f"{catalog_name}.gold.{table_name}"):
    summary_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.gold.{table_name}")
    spark.sql(f"ALTER TABLE {catalog_name}.gold.{table_name} CLUSTER BY AUTO;")
else:
    delta_table = DeltaTable.forName(spark, f"{catalog_name}.gold.{table_name}")
    delta_table.alias("gold_table").merge(summary_df.alias("data_snapshot"),"gold_table.date_id = data_snapshot.date_id AND gold_table.currency = data_snapshot.currency").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 
     