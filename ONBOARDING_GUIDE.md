# 🚀 Onboarding Guide: Adding a New Data Source

Welcome to the **Enterprise Retail Analytics Platform**! This guide outlines the standardized 4-step workflow to onboard new data sources. Our framework is **metadata-driven**, meaning you can add new sources without changing the underlying pipeline code.



---

## 🛠 Prerequisites
Ensure you have access to the **Metadata** folder within your Databricks workspace. All configuration notebooks are located there.

---

## 📋 The 4-Step Workflow

### Step 1: Register the Source
Open `01 Create Metadata Tables` and insert a new record into the `source_config` table.
* **Source Name**: Unique system identifier.
* **Source Path**: Location in ADLS Gen2.
* **Target Sink**: Destination path in the Silver/Gold layers.
* **Ingestion Frequency**: Define the load schedule (e.g., daily/weekly/incremental).

### Step 2: Define the Schema
In the `Schema Registry` section of the same notebook, define the structure:
* Add an entry for every column in your source file.
* Map incoming names to the target **Silver layer data types** (e.g., `String`, `Int`, `Decimal`).
* *Pro-tip: If your file has non-standard patterns, define custom parsing logic here.*

### Step 3: Set Quality Rules
Access the `Quality Rules` table to ensure data integrity before it reaches the Gold layer.
* **Non-null constraints**: Ensure critical IDs are not empty.
* **Range checks**: Set bounds for values like `Quantity` or `UnitPrice`.
* **Format validation**: Ensure date fields match the expected format (e.g., `YYYY-MM-DD`).

> 💡 **Why this matters:** Records that fail these rules are automatically flagged as `_IsRejected` and quarantined, ensuring your Gold layer remains a "Source of Truth."

### Step 4: Execute the Load
Open `02 Load Metadata` and run the notebook. It serves as the **Ingestion Engine** and performs the following automatically:
1. **Configuration Sync**: Detects your new source.
2. **Schema Enforcement**: Applies your defined data types.
3. **Quality Gate**: Activates your validation rules.
4. **End-to-End Processing**: Starts the flow through the Bronze $\rightarrow$ Silver $\rightarrow$ Gold layers.

---

## 📊 Summary of System Audit
Once processed, your data will automatically include these audit columns for full traceability:

| Audit Column | Purpose |
| :--- | :--- |
| `_AdfPipelineRunId` | Maps lineage to the ADF instance. |
| `_IngestionTimestamp` | Tracks entry time into the Data Lake. |
| `_ProcessedTimestamp` | Tracks Databricks transformation time. |
| `_IsRejected` | Flag for invalid/corrupt records. |

---

## 🆘 Troubleshooting
If the load fails, please check the following:
* Verify your `source_path` is correctly formatted in `source_config`.
* Ensure all required columns from your source file are registered in the `schema_registry`.
* Consult the `audit.pipeline_log` table for specific error messages regarding the failed execution.
