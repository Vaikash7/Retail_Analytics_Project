Onboarding Guide: Adding a New Data Source
To add a new data source to the Enterprise Retail Analytics Platform, follow this four-step standardized workflow. All configuration notebooks are located in the Metadata folder within your Databricks workspace.

Step 1: Register the New Source
Navigate to the Metadata folder in your workspace.

Open the notebook: 01 Create Metadata Tables.

Locate the source_config section and insert a new record for your data source.

Provide the following details:

Source Name: A unique identifier for the system.

Source Path: The specific location in ADLS Gen2 where the new data file will land.

Target Sink: The destination path where the data should reside in the Silver/Gold layers.

Ingestion Frequency: Define the load schedule (e.g., daily batch, weekly refresh, or incremental load).

Step 2: Define the Schema
Within the same metadata configuration, locate the Schema Registry section.

Add a new entry for every column present in your new source file.

Map each incoming column name to its target Silver layer data type (e.g., String, Int, Decimal).

If the file follows a non-standard structure or pattern, update the schema mapping to ensure the ingestion engine parses the file correctly.

Step 3: Set Quality Rules
Access the Quality Rules table within the metadata configuration.

Add specific validation rules for your new source to ensure data integrity before it reaches the Gold layer.

Common rules to apply include:

Non-null constraints: Ensure critical fields (like IDs) are never empty.

Range checks: Set minimum or maximum bounds for values like Quantity or UnitPrice.

Format validation: Specify if a field requires a strict date pattern (e.g., YYYY-MM-DD).

Step 4: Execute the Load
Open the notebook: 02 Load Metadata.

This notebook serves as your Ingestion Engine.

Run the notebook. It will perform the following actions automatically:

Configuration Sync: Detects the new source details you registered in Step 1.

Schema Enforcement: Applies the data types defined in Step 2.

Quality Gate: Activates the validation rules defined in Step 3.

End-to-End Processing: Automatically begins ingesting, cleaning, and transforming the data through your Bronze, Silver, and Gold layers.
