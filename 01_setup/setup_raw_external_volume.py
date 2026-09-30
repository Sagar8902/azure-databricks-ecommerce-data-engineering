# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG ecommerce;                     -- stay in this catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;           -- create the schema first
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS raw.raw_landing
# MAGIC   LOCATION 'abfss://raw@adlscbproject.dfs.core.windows.net/'
# MAGIC   COMMENT 'Raw ADLS Gen2 landing for ecommerce';