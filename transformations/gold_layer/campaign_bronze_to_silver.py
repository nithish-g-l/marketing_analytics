from pyspark import pipelines as dp

dp.create_streaming_table(
    name = "02_silver.dim_campaign"
)

dp.create_auto_cdc_flow(
    target = "02_silver.dim_campaign",
    source = "marketing_analytics_project.`01_bronze`.campaign",
    keys = ['campaign_id'],
    sequence_by="start_date",
    stored_as_scd_type=1
)
