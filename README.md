# 🚀 Enterprise Retail Analytics Platform

An end-to-end, metadata-driven Data Engineering solution built on Microsoft Azure. This platform automates the ingestion, transformation, and validation of retail data, ensuring a reliable "Source of Truth" from raw ingestion to business-ready reporting.

---

## 🏗 Architecture Overview
The platform utilizes a **Medallion Architecture** to process data efficiently across the Azure ecosystem.

* **Data Sources**: CSV Files, Azure SQL, and External APIs.
* **Orchestration**: Azure Data Factory (ADF) for pipeline scheduling and control.
* **Processing & Storage**: Azure Data Lake Storage (ADLS Gen2) and Azure Databricks (Spark).
* **Serving**: Power BI for business intelligence and reporting.

---

## 🛠 Tech Stack
* **Cloud**: Microsoft Azure (ADLS Gen2, Data Factory, Databricks).
* **Languages**: Python (PySpark), SQL.
* **Framework**: Custom Metadata-Driven Ingestion Engine.
* **Visualization**: Power BI.

---

## 🔑 Key Features
* **Metadata-Driven Design**: Onboard new data sources by updating configuration tables in Databricks—no code changes required.
* **Automated Quality Gates**: Built-in validation checks (non-null, range, format) to quarantine bad records before reaching the Gold layer.
* **Full Data Lineage**: Automated audit columns (`_AdfPipelineRunId`, `_IngestionTimestamp`) provide total traceability.
* **Scalable Storage**: Medallion architecture (Landing $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold) ensures clean, structured data.

---

## 🚀 Getting Started
1. **Initialize Resources**: Deploy ADLS Gen2, Data Factory, and Databricks.
2. **Configure Linked Services**: Set up secure connections in ADF.
3. **Register Sources**: Use the `01_Create_Metadata_Tables` notebook to register your source schema.
4. **Run Master Pipeline**: Execute `PL_MASTER_PIPELINE` to trigger the automated end-to-end flow.

---

## 📂 Project Structure
```text
/Retail_Analytics_Project
│
├── /Framework           # Core logic for Bronze, Silver, and Gold processing
├── /Metadata            # Configuration notebooks for source registration
├── /Source_Generator    # Mock data generation for testing
└── /ADF_Pipelines       # Master orchestration and ingestion workflows
