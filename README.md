# azure-databricks-ecommerce-data-engineering
End-to-end Azure Databricks data engineering project implementing batch and streaming data pipelines using ADLS Gen2, Unity Catalog, Delta Lake, Medallion Architecture, Change Data Feed, PySpark, automated workflows, and Power BI


# 🚀 E-Commerce Data Engineering with Azure Databricks

![Azure](https://img.shields.io/badge/Azure-Cloud-blue)
![Databricks](https://img.shields.io/badge/Azure%20Databricks-Data%20Engineering-red)
![PySpark](https://img.shields.io/badge/PySpark-ETL-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-blue)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)

## 📌 Project Overview

This project is an **end-to-end E-Commerce Data Engineering pipeline** built using Azure Databricks.

The pipeline takes raw e-commerce data, processes and cleans it through different layers, and creates business-ready data for Power BI reporting.

### 🎯 Main Objectives

- Build an end-to-end data pipeline
- Process batch and incremental data
- Clean and transform data using PySpark
- Use Delta Lake for reliable data storage
- Implement Change Data Feed (CDF)
- Process changes using Structured Streaming
- Automate pipelines using Databricks Workflows
- Create analytics-ready data for Power BI

---

# 🏗️ Project Architecture

The project follows the **Medallion Architecture**.

```text
                         E-COMMERCE DATA
                                │
                                ▼
                       ┌─────────────────┐
                       │    ADLS Gen2    │
                       │   Raw Storage   │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  🥉 BRONZE      │
                       │    Raw Data     │
                       │  Delta Tables   │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  🥈 SILVER      │
                       │Clean & Transform│
                       │  Data Quality   │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   🥇 GOLD       │
                       │ Business-Ready  │
                       │      Data       │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │    POWER BI     │
                       │    Dashboard    │

                       └─────────────────┘

## 📥 Explore the Databricks Project

To explore the complete Databricks project:

1. Download the [`databrick.dbc`](https://github.com/Sagar8902/azure-databricks-ecommerce-data-engineering/blob/main/04_databrick_dbc_file/databrick.dbc) file from the **`04_databrick_dbc_file`** folder.
2. Import the `.dbc` file into your **Azure Databricks workspace**.
3. Open the notebooks and explore the complete data engineering pipeline.

### 📊 Data Flow & Architecture

Check the [`05_dataflow_diagram`](https://github.com/Sagar8902/azure-databricks-ecommerce-data-engineering/tree/main/05_dataflow_diagram) folder to understand:

- 🔄 Data Flow
- 🔗 Data Lineage
- ⚙️ Data Pipeline
- 🏗️ Project Architecture
- 📊 Power BI Dashboard


💼 Project Description

Built an end-to-end E-Commerce Data Engineering pipeline using Azure Databricks, ADLS Gen2, PySpark and Delta Lake. Implemented Medallion Architecture, batch and streaming processing, Change Data Feed, MERGE/UPSERT, data quality checks, automated workflows and Power BI reporting.

👨‍💻 Author
Sagar Soni

Data Engineer | SQL | Python | PySpark | Azure | Databricks

