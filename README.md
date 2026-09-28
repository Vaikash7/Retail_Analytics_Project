# 🚀 Enterprise Retail Analytics Platform

> **An end-to-end, metadata-driven Data Engineering platform built on Microsoft Azure to automate data ingestion, transformation, validation, and delivery of business-ready retail data.**

<p align="center">

![Azure](https://img.shields.io/badge/Microsoft%20Azure-Cloud-0078D4?style=for-the-badge\&logo=microsoftazure\&logoColor=white)
![ADF](https://img.shields.io/badge/Azure%20Data%20Factory-Orchestration-0078D4?style=for-the-badge\&logo=microsoftazure\&logoColor=white)
![Databricks](https://img.shields.io/badge/Azure%20Databricks-PySpark-FF3621?style=for-the-badge\&logo=databricks\&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Processing-E25A1C?style=for-the-badge\&logo=apachespark\&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Data%20Engineering-336791?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-F2C811?style=for-the-badge\&logo=powerbi\&logoColor=black)

</p>

---

## 📌 Table of Contents

<details>
<summary><b>Click to expand</b></summary>

* [Project Overview](#-project-overview)
* [Business Problem](#-business-problem)
* [Architecture](#-architecture)
* [End-to-End Data Flow](#-end-to-end-data-flow)
* [Tech Stack](#-tech-stack)
* [Data Sources](#-data-sources)
* [Medallion Architecture](#-medallion-architecture)
* [Metadata-Driven Framework](#-metadata-driven-framework)
* [ADF Orchestration](#-adf-orchestration)
* [PySpark Transformations](#-pyspark-transformations)
* [Data Quality](#-data-quality)
* [Schema Evolution](#-schema-evolution)
* [Incremental Processing](#-incremental-processing)
* [Audit & Data Lineage](#-audit--data-lineage)
* [Project Structure](#-project-structure)
* [Getting Started](#-getting-started)
* [Sample Data Flow](#-sample-data-flow)
* [Key Engineering Concepts](#-key-engineering-concepts)
* [What I Learned](#-what-i-learned)
* [Future Enhancements](#-future-enhancements)
* [Author](#-author)

</details>

---

# 📖 Project Overview

The **Enterprise Retail Analytics Platform** is an end-to-end Azure Data Engineering project designed to create a reliable **single source of truth** from multiple retail data sources.

The platform follows a **Medallion Architecture**:

```text
Landing → Bronze → Silver → Gold
```

Azure Data Factory handles orchestration and ingestion, while Azure Databricks and PySpark perform data transformation and processing.

The framework is **metadata-driven**, allowing new sources to be onboarded through configuration instead of requiring pipeline code changes.

---

# 🎯 Business Problem

Retail organizations receive data from multiple systems and formats.

Typical challenges include:

* Multiple data sources
* Different file formats
* Changing schemas
* Duplicate records
* Missing values
* Invalid records
* Repeated manual pipeline development
* Difficult data lineage
* Incremental data processing

This project addresses these challenges through an automated and reusable Data Engineering framework.

---

# 🏗️ Architecture

```text
                         DATA SOURCES
                              │
             ┌────────────────┼────────────────┐
             │                │                │
             ▼                ▼                ▼
        Orders CSV       Products CSV     Azure SQL
             │                │                │
             └────────────────┼────────────────┘
                              │
                              ▼
                       External REST API
                              │
                              ▼
                    ┌─────────────────────┐
                    │ Azure Data Factory  │
                    │                     │
                    │ Master Pipeline     │
                    │ Metadata Driven     │
                    │ Orchestration       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │     ADLS Gen2       │
                    │                     │
                    │     Landing         │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      BRONZE         │
                    │                     │
                    │ Raw Ingested Data   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ Azure Databricks    │
                    │      PySpark        │
                    │                     │
                    │ Transformation      │
                    │ Validation          │
                    │ Deduplication       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      SILVER         │
                    │                     │
                    │ Cleaned & Validated │
                    │ Data                │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       GOLD          │
                    │                     │
                    │ Business-Ready Data │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Power BI       │
                    │                     │
                    │ Reports & Analytics │
                    └─────────────────────┘
```

---

# 🔄 End-to-End Data Flow

```text
Data Sources
     │
     ▼
Azure Data Factory
     │
     ▼
Metadata Configuration
     │
     ▼
Landing Layer
     │
     ▼
Bronze Layer
     │
     ▼
Databricks / PySpark
     │
     ├── Data Cleaning
     ├── Deduplication
     ├── Type Casting
     ├── Null Handling
     ├── Validation
     └── Business Transformations
     │
     ▼
Silver Layer
     │
     ▼
Gold Layer
     │
     ▼
Power BI
```

---

# 🛠️ Tech Stack

| Technology                | Purpose                        |
| ------------------------- | ------------------------------ |
| ☁️ **Azure**              | Cloud platform                 |
| 🔄 **Azure Data Factory** | Data ingestion & orchestration |
| 🗄️ **ADLS Gen2**         | Cloud data lake storage        |
| 🔥 **Azure Databricks**   | Data processing                |
| ⚡ **PySpark**             | Distributed transformations    |
| 🗃️ **SQL**               | Data querying & transformation |
| 📊 **Power BI**           | Business reporting             |
| 🐍 **Python**             | Data processing & automation   |
| 🌐 **REST API**           | External data ingestion        |

---

# 📥 Data Sources

The project integrates multiple types of sources.

### 🛒 Orders

```text
Source: CSV
Frequency: Daily
```

Example:

```text
Order_ID
Customer_ID
Product_ID
Order_Date
Quantity
Price
```

---

### 👥 Customers

```text
Source: Azure SQL
```

Contains customer master information.

---

### 📦 Products

```text
Source: CSV
Frequency: Weekly
```

Contains product information such as:

```text
Product_ID
Product_Name
Category
Price
```

---

### 💱 Exchange Rates

```text
Source: REST API
```

External exchange-rate information is integrated into the data pipeline.

---

# 🥉 Medallion Architecture

The platform uses three major processing layers.

## 🟤 Bronze Layer

Stores data in its raw or minimally processed form.

```text
Source
  ↓
Bronze
  ↓
Raw Data
```

Characteristics:

* Raw ingestion
* Minimal transformation
* Source traceability
* Historical preservation

---

## ⚪ Silver Layer

The Silver layer contains cleaned and validated data.

Typical operations include:

```text
Duplicate Removal
       ↓
Null Handling
       ↓
Data Type Conversion
       ↓
Schema Validation
       ↓
Business Rules
       ↓
Clean Data
```

---

## 🟡 Gold Layer

The Gold layer contains business-ready datasets designed for analytics and reporting.

```text
Silver
   ↓
Business Transformations
   ↓
Aggregations
   ↓
Gold
   ↓
Power BI
```

---

# ⚙️ Metadata-Driven Framework

One of the key features of this project is the **metadata-driven ingestion framework**.

Instead of creating separate pipelines for every source, source information is maintained in configuration metadata.

Example:

```text
┌─────────────────────────────────────┐
│       Metadata Configuration        │
├──────────────┬──────────────────────┤
│ Source       │ Target               │
├──────────────┼──────────────────────┤
│ Orders       │ Bronze/Orders        │
│ Customers    │ Bronze/Customers     │
│ Products     │ Bronze/Products      │
│ ExchangeRate │ Bronze/ExchangeRate  │
└──────────────┴──────────────────────┘
```

The pipeline reads the metadata and dynamically determines:

* Source location
* File name
* Target location
* Source type
* Load type
* Processing requirements

### Main Advantage

```text
Traditional Approach

New Source
   ↓
Write New Pipeline
   ↓
Modify Code
   ↓
Test
   ↓
Deploy


Metadata-Driven Approach

New Source
   ↓
Add Metadata
   ↓
Existing Pipeline
   ↓
Automatically Process
```

This improves **reusability and maintainability**.

---

# 🔄 ADF Orchestration

Azure Data Factory acts as the orchestration layer.

The master pipeline controls the complete workflow.

```text
                  MASTER PIPELINE
                        │
            ┌───────────┴───────────┐
            │                       │
            ▼                       ▼
       Read Metadata          Check Source
            │                       │
            └───────────┬───────────┘
                        ▼
                  Ingest Data
                        │
                        ▼
                     Bronze
                        │
                        ▼
                  Databricks
                        │
                        ▼
                    Silver
                        │
                        ▼
                     Gold
                        │
                        ▼
                   Validation
                        │
                        ▼
                   Completion
```

---

# 🔥 PySpark Transformations

Azure Databricks and PySpark are used for distributed data processing.

### Remove Duplicates

```python
df = df.dropDuplicates(["id"])
```

### Handle Null Values

```python
df = df.dropna()
```

or:

```python
df = df.fillna("unknown")
```

### Add Columns

```python
from pyspark.sql.functions import current_timestamp

df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)
```

### Filtering

```python
df = df.filter(df.quantity > 0)
```

### Aggregation

```python
df.groupBy("product_id").count()
```

### Sorting

```python
df.sort("product_id")
```

---

# 🧹 Data Quality

The framework includes validation checks before data reaches the Gold layer.

Example validation rules:

```text
                Incoming Data
                     │
                     ▼
             ┌───────────────┐
             │ Data Quality  │
             │    Checks     │
             └───────┬───────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
    NOT NULL       Range       Format
        │            │            │
        └────────────┼────────────┘
                     ▼
               Valid Records
                     │
                     ▼
                   Silver
```

Invalid records can be separated into a **reject/quarantine area** instead of allowing bad data to continue into the business layer.

---

# 🔄 Schema Evolution

The pipeline is designed to handle changes in incoming schemas.

For example:

### Original File

```text
Customer_ID
Customer_Name
Customer_Email
```

### New File

```text
Customer_ID
Customer_Name
Customer_Email
Customer_Phone
```

Instead of failing because of the new column, the framework can detect the schema difference and accommodate the additional field.

```text
Existing Schema
       +
New Source Schema
       │
       ▼
Compare Schemas
       │
       ▼
Identify New Columns
       │
       ▼
Update Target Schema
       │
       ▼
Continue Processing
```

---

# 📈 Incremental Processing

The platform supports incremental processing to avoid unnecessarily processing the entire dataset.

A watermark or last-processed value can be used to identify new records.

```text
Previous Load
      │
      ▼
Last Processed Timestamp
      │
      ▼
New Incoming Records
      │
      ▼
Process Only New Data
```

This improves processing efficiency for growing datasets.

---

# 🧾 Audit & Data Lineage

The platform maintains audit information to improve traceability.

Example audit columns:

```text
_AdfPipelineRunId
_IngestionTimestamp
```

These fields help answer:

* Which pipeline loaded this record?
* When was the record ingested?
* Which pipeline execution produced the data?
* Where did the data originate?

Example:

```text
Customer_ID | Data | _AdfPipelineRunId | _IngestionTimestamp
----------------------------------------------------------------
1001        | ...  | 8f73...           | 2026-09-25 10:30
```

---

# 📂 Project Structure

```text
Retail_Analytics_Project/
│
├── README.md
│
├── docs/
│   └── onboarding-guide.md
│
└── Retail_Analytics_Project/
    │
    ├── Framework/
    │   │
    │   ├── Bronze/
    │   ├── Silver/
    │   ├── Gold/
    │   ├── Data_Quality/
    │   └── Utilities/
    │
    ├── Metadata/
    │   │
    │   ├── 01_Create_Metadata_Tables
    │   ├── Source_Configuration
    │   └── Metadata_Management
    │
    ├── Source_Generator/
    │   │
    │   ├── Orders/
    │   ├── Customers/
    │   ├── Products/
    │   └── Exchange_Rates/
    │
    └── ADF_Pipelines/
        │
        ├── Master_Pipeline/
        ├── Ingestion/
        └── Transformation/
```

---

# 📁 Framework

The `Framework` directory contains the reusable processing logic.

```text
Framework/
│
├── Bronze/
│   └── Raw ingestion processing
│
├── Silver/
│   └── Cleaning & transformation
│
├── Gold/
│   └── Business transformations
│
└── Data_Quality/
    └── Validation rules
```

---

# ⚙️ Metadata

The `Metadata` directory contains configuration used by the metadata-driven framework.

```text
Metadata/
│
├── 01_Create_Metadata_Tables
├── Source_Configuration
└── Metadata_Management
```

New sources can be registered through metadata rather than creating a completely new pipeline.

---

# 🧪 Source Generator

The `Source_Generator` directory contains scripts/notebooks used to generate sample retail datasets for development and testing.

```text
Source_Generator/
│
├── Orders
├── Customers
├── Products
└── Exchange_Rates
```

---

# 🔄 ADF Pipeline Structure

```text
ADF_Pipelines/
│
├── Master Pipeline
│
├── Source Ingestion
│
├── Bronze Processing
│
├── Silver Processing
│
└── Gold Processing
```

The **Master Pipeline** acts as the main orchestration layer.

---

# 🚀 Getting Started

## 1️⃣ Initialize Azure Resources

Create the required Azure resources:

```text
Azure Data Lake Storage Gen2
Azure Data Factory
Azure Databricks
Azure SQL
```

---

## 2️⃣ Configure ADLS

Create the required folder structure:

```text
Retail_Analytics/
│
├── landing/
├── bronze/
├── silver/
├── gold/
├── metadata/
├── logs/
├── rejects/
└── checkpoints/
```

---

## 3️⃣ Configure ADF Linked Services

Create connections for:

```text
ADF
 │
 ├── ADLS Gen2
 ├── Azure SQL
 └── Databricks
```

---

## 4️⃣ Create Metadata Tables

Run:

```text
01_Create_Metadata_Tables
```

Register the required sources.

Example:

```text
Orders
Customers
Products
Exchange_Rates
```

---

## 5️⃣ Run Master Pipeline

Execute:

```text
PL_MASTER_PIPELINE
```

The master pipeline automatically orchestrates the required ingestion and processing steps.

---

# 📊 Sample Data Flow

### Orders

```text
Orders CSV
    │
    ▼
Azure Data Factory
    │
    ▼
ADLS Landing
    │
    ▼
Bronze
    │
    ▼
PySpark
    │
    ├── Remove Duplicates
    ├── Handle Nulls
    ├── Type Casting
    └── Validation
    │
    ▼
Silver
    │
    ▼
Business Aggregations
    │
    ▼
Gold
    │
    ▼
Power BI
```

---

# 🧠 Key Engineering Concepts

This project demonstrates practical experience with:

### Azure

* Azure Data Factory
* ADLS Gen2
* Azure Databricks
* Azure SQL

### Data Engineering

* ETL / ELT
* Data ingestion
* Batch processing
* Incremental processing
* Metadata-driven frameworks
* Data validation
* Data lineage
* Audit logging
* Schema evolution

### PySpark

* DataFrames
* Transformations
* Filtering
* Aggregations
* Deduplication
* Null handling
* Type casting

### Architecture

* Medallion Architecture
* Bronze / Silver / Gold
* Metadata-driven architecture
* Reusable pipelines

---

# 📸 Screenshots

You can add screenshots from your Azure environment here.

Recommended structure:

```text
docs/
│
└── screenshots/
    ├── adf-pipeline.png
    ├── adls-structure.png
    ├── databricks-notebook.png
    ├── metadata-table.png
    ├── bronze-layer.png
    ├── silver-layer.png
    └── gold-layer.png
```

Then add:

```markdown
## 📸 Project Screenshots

### Azure Data Factory

![ADF Pipeline](docs/screenshots/adf-pipeline.png)

### ADLS Gen2

![ADLS Structure](docs/screenshots/adls-structure.png)

### Databricks

![Databricks Notebook](docs/screenshots/databricks-notebook.png)

### Metadata Configuration

![Metadata](docs/screenshots/metadata-table.png)
```

---

# 🎓 What I Learned

Through this project, I gained hands-on experience in:

* Designing an end-to-end Azure Data Engineering architecture
* Building data ingestion pipelines using Azure Data Factory
* Working with ADLS Gen2
* Processing large datasets using PySpark
* Implementing Medallion Architecture
* Building metadata-driven ingestion frameworks
* Handling duplicates and missing data
* Implementing data quality validation
* Handling schema evolution
* Implementing incremental data processing
* Maintaining audit columns and data lineage
* Preparing business-ready datasets for analytics

---

# 🔮 Future Enhancements

Potential improvements include:

* [ ] Add automated CI/CD using Azure DevOps or GitHub Actions
* [ ] Add comprehensive pipeline monitoring
* [ ] Add automated data quality reports
* [ ] Add Power BI dashboards
* [ ] Implement SCD Type 2 for customer dimensions
* [ ] Add centralized error logging
* [ ] Add automated email/Teams failure notifications
* [ ] Implement Azure Key Vault for secrets
* [ ] Add unit tests for PySpark transformations
* [ ] Add performance optimization using partitioning and caching

---

# ⭐ Project Highlights

```text
┌───────────────────────────────────────────────┐
│       ENTERPRISE RETAIL ANALYTICS             │
├───────────────────────────────────────────────┤
│                                               │
│  ✓ Metadata-Driven Architecture              │
│  ✓ Medallion Architecture                    │
│  ✓ Automated ADF Orchestration                │
│  ✓ Azure Data Lake Storage                   │
│  ✓ Databricks + PySpark Processing           │
│  ✓ Data Quality & Validation                 │
│  ✓ Schema Evolution                          │
│  ✓ Incremental Processing                   │
│  ✓ Audit & Data Lineage                     │
│  ✓ Business-Ready Gold Layer                │
│                                               │
└───────────────────────────────────────────────┘
```

---

# 👨‍💻 Author

## Chatrathi Vaikash

**Computer Science & Engineering Graduate**

### 💻 Data Engineering Skills

```text
Azure | ADF | ADLS Gen2
Databricks | PySpark | SQL
Snowflake | Python
```

### 🔗 GitHub

[![GitHub](https://img.shields.io/badge/GitHub-Vaikash7-181717?style=for-the-badge\&logo=github)](https://github.com/Vaikash7)

---

⭐ **If you found this project useful, consider giving the repository a star!**
