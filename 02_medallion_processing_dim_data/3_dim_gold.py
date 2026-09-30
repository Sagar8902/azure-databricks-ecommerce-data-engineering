# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver to Gold: Building BI Ready Tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products 

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------


# Make sure we’re on the right catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Build brands×category mapping and write Gold table
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brands_categories AS (
# MAGIC   SELECT
# MAGIC     b.brand_name,
# MAGIC     b.brand_code,
# MAGIC     c.category_name,
# MAGIC     c.category_code
# MAGIC   FROM v_brands b
# MAGIC   INNER JOIN v_category c
# MAGIC   ON
# MAGIC     b.category_code = c.category_code
# MAGIC )
# MAGIC SELECT
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'Not Available') AS category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available')   AS brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating_count,
# MAGIC   p.file_name,
# MAGIC   p.ingest_timestamp
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brands_categories bc
# MAGIC   ON p.brand_code = bc.brand_code;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

# 1 Flatten country_state_map into a list of Rows
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]        

# COMMAND ----------

# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)




# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customers')


# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})



# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date/Calendar

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_calendar')


# COMMAND ----------

df_gold = df_silver.withColumn("date_id", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", F.date_format(F.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    F.when(F.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)




# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)



# COMMAND ----------

# write table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")