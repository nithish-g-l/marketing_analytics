# marketing_analytics
This repository contains an end-to-end Marketing Analytics solution that:

Ingests raw advertising and conversion data into the Bronze layer.
Applies data cleansing, validation, CDC processing, and quality checks in the Silver layer.
Builds aggregated Gold-layer data models for business reporting.
Calculates key marketing KPIs such as:
Impressions
Clicks
CTR (Click-Through Rate)
Conversion Rate
Spend
Revenue
ROAS (Return on Ad Spend)
Uses Delta Live Tables (DLT) for orchestration, monitoring, and data quality enforcement.
Follows the Medallion Architecture for scalable and reliable data processing.

Tech Stack: PySpark, Databricks, Delta Lake, Delta Live Tables (DLT), SQL, Medallion Architecture.
