from pyspark import pipelines as dp
from pyspark.sql.functions import *

QC = {
    "valid_clicks" : "clicks < impressions",
    "valid_spend" : "spend >= 0.00"
}

@dp.table(
    name = '02_silver.fact_ad_performance'
)

@dp.expect_all_or_drop(QC)

def fact_ad_performance():
    df = spark.readStream.table("marketing_analytics_project.`01_bronze`.ad_performance")
    df_trans = (
        df.dropDuplicates(["campaign_id", "date", "device"])
        .fillna({
            'impressions' : 0,
            'clicks' : 0,
            'spend' : 0.00
        })
        .where('impressions > 0')
        .withColumn('spend', col('spend')/lit(95))
    )
    return df_trans
