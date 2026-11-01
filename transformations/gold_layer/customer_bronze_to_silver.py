from pyspark import pipelines as dp

dp.create_streaming_table(
    name = "02_silver.dim_customer"
)

dp.create_auto_cdc_flow(
    target = "02_silver.dim_customer",
    source = "marketing_analytics_project.`01_bronze`.customer",
    keys = ['customer_id'],
    sequence_by="customer_id",
    stored_as_scd_type=1
)
