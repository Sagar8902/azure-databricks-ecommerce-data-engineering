# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce
# MAGIC MANAGED LOCATION 'abfss://metadata@adlscbproject.dfs.core.windows.net/ecommerce-catalog'
# MAGIC COMMENT 'Ecommerce project catalog in Central India, backed by external location';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM ecommerce;