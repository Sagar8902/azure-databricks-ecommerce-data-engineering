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
                       │ Clean & Transform│
                       │  Data Quality   │
                       └────────┬────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   STREAMING     │
                       │  PySpark / CDF  │
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
