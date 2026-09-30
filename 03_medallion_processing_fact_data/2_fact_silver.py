# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze to Silver: Data Cleansing and Transformation

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "adlscbproject", "Storage Account Name")
dbutils.widgets.text("container_name", "raw", "Container Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Bronze Table in a Dataframe

# COMMAND ----------

df = spark.read \
.format("delta") \
.table(f"{catalog_name}.bronze.brz_order_items")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform Transformations and Cleaning

# COMMAND ----------

df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' → 2 and cast to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df = df.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Transformation : Remove '%' from discount_pct and cast to double
df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Transformation : coupon code processing (convert to lower)
df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Transformation : channel processing 
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

#Transformation : Add processed time 
df = df.withColumn(
    "processed_time", F.current_timestamp()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Save to Silver Table

# COMMAND ----------

# ============================================================
# BRONZE → SILVER
# Write cleaned/transformed DataFrame to Silver Delta Table
# ============================================================

# ------------------------------------------------------------
# 1. Silver table name
# ------------------------------------------------------------

silver_table = f"{catalog_name}.silver.slv_order_items"

print("Silver table:", silver_table)


# ------------------------------------------------------------
# 2. Check whether df is streaming
# ------------------------------------------------------------

print("Is df streaming?", df.isStreaming)


# ------------------------------------------------------------
# 3. Write transformed Bronze DataFrame to Silver
# ------------------------------------------------------------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created successfully: {silver_table}")


# ------------------------------------------------------------
# 4. Enable Change Data Feed
# ------------------------------------------------------------

spark.sql(
    f"""
    ALTER TABLE {silver_table}
    SET TBLPROPERTIES (
        delta.enableChangeDataFeed = true
    )
    """
)

print("Change Data Feed enabled.")

