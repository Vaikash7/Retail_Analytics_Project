# 🏗 End-to-End Platform Setup Guide

This guide details the sequential steps to build and navigate your **Retail Analytics Platform** on Azure.

---

## Step 1: Resource Initialization
Before data flows, you must establish the Azure infrastructure:

* **Storage Account (ADLS Gen2)**: This serves as your centralized data repository. Within the storage account, you create a container (e.g., `data`) to host your folder structure (Landing, Bronze, Silver, Gold, Metadata, etc.).
* **Azure Databricks**: This is your processing engine. Create a workspace to host your notebooks, which are organized into logical folders such as `Framework`, `Metadata`, and `Source_Generator`.
* **Azure Data Factory (ADF)**: This is your orchestration tool. Create an ADF instance to build, trigger, and monitor your data pipelines.

---

## Step 2: Establishing Connections (Linked Services)
A **Linked Service** in ADF acts as your "connection string" with authentication.

* **Why use it**: Instead of hard-coding credentials for every task, you define a Linked Service once (e.g., connecting ADF to ADLS or Databricks). This ensures security and allows you to reuse the connection across multiple pipelines.
* **Navigation**: Inside ADF, go to the **Manage** tab (the toolbox icon), select **Linked services**, and click **New** to connect to your Azure Blob Storage or Databricks instance.

---

## Step 3: Pipeline Orchestration
Pipelines move and transform your data. You can navigate to the **Author** tab (pencil icon) to create new pipelines.

* **Creating Pipelines**: Drag and drop activities (like *Copy Data* or *Notebook*) onto the canvas.
* **Master Pipeline**: Your `PL_MASTER_PIPELINE` acts as the orchestrator. It sequences the execution of your ingestion pipelines (e.g., `PL_HTTP_TO_ADLS`) and triggers your Databricks `Master_Framework` notebook to process the data.

---

## Step 4: Metadata Management
Your framework is **metadata-driven**, meaning you avoid manual coding for every new file.

* **Registration**: Open your Databricks `Metadata` folder and use the `01_Create_Metadata_Tables` notebook to register new file patterns and schemas.
* **Navigation**: Use the **Catalog** explorer in Databricks to view your managed tables, volumes, and schemas (e.g., `retail_catalog.default`) to verify that your data has landed in the correct layer.

---

## 📊 Summary of Core Components

| Component | Purpose |
| :--- | :--- |
| **ADLS Gen2** | Provides the scalable "Data Lake" storage for raw to gold data. |
| **ADF** | Orchestrates the movement of data between sources and the Lake. |
| **Databricks** | Processes the data using Spark for heavy transformations. |
| **Linked Services** | Simplifies management by centralizing connection credentials. |

---

## 💡 Pro-Tips for Navigation

* **Use the Workspace**: Keep your notebooks organized into folders (`Framework`, `Metadata`, `Source_Generator`) to make finding scripts easier.
* **Check Activity Outputs**: If a pipeline fails, click the **Output** tab at the bottom of the ADF canvas to view specific error messages.
* **Leverage the Framework**: Always remember—you don't write new code for new files. You simply add a row to your metadata tables, and the `Master_Framework` does the heavy lifting.
