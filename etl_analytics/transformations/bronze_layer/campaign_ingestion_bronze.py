from pyspark import pipelines as dp
from pyspark.sql.functions import *

VOL_LOCATION = spark.conf.get("camp_loc")

SCHEMA =  """
    campaign_id STRING,
    campaign_name STRING,
    channel STRING,
    start_date DATE,
    budget_usd DOUBLE
"""

@dp.table(
    name = 'campaign'
)

def campaign():
    df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option('header','true')
    .schema(SCHEMA)
    .load(VOL_LOCATION)
    )
    return df.drop('_rescued_data')
