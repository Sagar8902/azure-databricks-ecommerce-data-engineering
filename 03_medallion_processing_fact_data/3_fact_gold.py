# Databricks notebook source
# MAGIC %md
# MAGIC ### From Silver To Gold: Aggregation and KPI Tables

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets

# COMMAND ----------

dbutils.widgets.text("catalog_name", "ecommerce", "Catalog Name")
dbutils.widgets.text("storage_account_name", "adlscbproject", "Storage Account Name")
dbutils.widgets.text("container_name", "raw", "Container Name")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
storage_account_name = dbutils.widgets.get("storage_account_name")
container_name = dbutils.widgets.get("container_name")

print(catalog_name, storage_account_name, container_name)

# COMMAND ----------

# ============================================================
# SILVER → CDF STREAM
# ============================================================

silver_table = f"{catalog_name}.silver.slv_order_items"

silver_cdf_checkpoint_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    "checkpoint/cdf/silver_order_items_v2/"
)

print("Silver table:", silver_table)
print("CDF checkpoint:", silver_cdf_checkpoint_path)


# Read Silver Change Data Feed
cdf_df = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .table(silver_table)
)



# COMMAND ----------

df_union = cdf_df.filter("_change_type IN ('insert', 'update_postimage')")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# ============================================================
# 1. Calculate gross amount
# ============================================================

df_union = df_union.withColumn(
    "gross_amount",
    F.col("quantity") * F.col("unit_price")
)


# ============================================================
# 2. Calculate discount amount
# discount_pct = 21 means 21%
# ============================================================

df_union = df_union.withColumn(
    "discount_amount",
    F.ceil(
        F.col("gross_amount") *
        (F.col("discount_pct") / F.lit(100.0))
    )
)


# ============================================================
# 3. Calculate sale amount
# sale_amount = gross_amount - discount_amount + tax_amount
# ============================================================

df_union = df_union.withColumn(
    "sale_amount",
    F.col("gross_amount")
    - F.col("discount_amount")
    + F.col("tax_amount")
)


# ============================================================
# 4. Create date_id
# Example:
# 2026-09-30 → 20260930
# ============================================================

df_union = df_union.withColumn(
    "date_id",
    F.date_format(
        F.col("dt"),
        "yyyyMMdd"
    ).cast(IntegerType())
)


# ============================================================
# 5. Create coupon flag
# coupon exists    → 1
# coupon is NULL   → 0
# ============================================================

df_union = df_union.withColumn(
    "coupon_flag",
    F.when(
        F.col("coupon_code").isNotNull(),
        F.lit(1)
    )
    .otherwise(F.lit(0))
)


# ============================================================
# 6. Check whether DataFrame is streaming
# ============================================================

print("Is df_union streaming?", df_union.isStreaming)


# ============================================================
# 7. Display streaming DataFrame
# IMPORTANT:
# Use a dedicated checkpoint for this display query
# ============================================================

gold_preview_checkpoint = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    "checkpoint/preview/gold_order_items/"
)

print("Preview checkpoint:", gold_preview_checkpoint)




# COMMAND ----------

orders_gold_df = df_union.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Gold Table

# COMMAND ----------

from delta.tables import DeltaTable


# ============================================================
# 1. GOLD CHECKPOINT
# ============================================================

gold_checkpoint_path = (
    f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    "checkpoint/gold/fact_order_items/"
)

print("Gold checkpoint:", gold_checkpoint_path)


# ============================================================
# 2. GOLD TABLE
# ============================================================

gold_table = f"{catalog_name}.gold.gld_fact_order_items"

print("Gold table:", gold_table)


# ============================================================
# 3. UPSERT FUNCTION
# ============================================================

def upsert_to_gold(microBatchDF, batchId):

    print(f"Processing Gold batch: {batchId}")

    # --------------------------------------------------------
    # Handle empty micro-batch
    # --------------------------------------------------------

    if microBatchDF.isEmpty():

        print(f"Batch {batchId} is empty. Nothing to process.")

        return


    # --------------------------------------------------------
    # Create Gold table if it does not exist
    # --------------------------------------------------------

    if not spark.catalog.tableExists(gold_table):

        print("Gold table does not exist.")
        print("Creating Gold table...")

        (
            microBatchDF.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table)
        )

        # Enable Change Data Feed
        spark.sql(
            f"""
            ALTER TABLE {gold_table}
            SET TBLPROPERTIES (
                delta.enableChangeDataFeed = true
            )
            """
        )

        print("Gold table created successfully.")


    # --------------------------------------------------------
    # Table already exists → MERGE
    # --------------------------------------------------------

    else:

        print("Gold table already exists.")
        print(f"Merging batch {batchId}...")


        deltaTable = DeltaTable.forName(
            spark,
            gold_table
        )


        (
            deltaTable.alias("gold_table")
            .merge(
                microBatchDF.alias("batch_table"),

                """
                gold_table.transaction_id = batch_table.transaction_id
                AND
                gold_table.seq_no = batch_table.seq_no
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


        print(f"Batch {batchId} merged successfully.")


# ============================================================
# 4. CHECK SOURCE
# ============================================================

print(
    "Is orders_gold_df streaming?",
    orders_gold_df.isStreaming
)


# ============================================================
# 5. START GOLD STREAMING JOB
# ============================================================

query = (
    orders_gold_df
    .writeStream
    .foreachBatch(upsert_to_gold)
    .option(
        "checkpointLocation",
        gold_checkpoint_path
    )
    .trigger(
        availableNow=True
    )
    .start()
)


# ============================================================
# 6. WAIT FOR COMPLETION
# ============================================================

query.awaitTermination()


print("Gold streaming job completed.")