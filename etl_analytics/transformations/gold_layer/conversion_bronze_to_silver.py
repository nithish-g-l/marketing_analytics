from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table(
    name='02_silver.fact_conversion'
)

def fact_conversion():
    df = (spark.readStream.table("marketing_analytics_project.`01_bronze`.conversions")
      .withColumn('date', to_date(col('date'))
                  )
      .withColumn('conversion', from_json(col('conversion'), 'array<struct<customer_id:string,conversion_type: STRING,revenue_usd: DOUBLE>>'))
      .withColumn('conversion', explode(col('conversion')))
      .withColumn('revenue_usd', col('conversion.revenue_usd'))
      .withColumn('conversion_type', col('conversion.conversion_type'))
      .withColumn('customer_id', col('conversion.customer_id'))
      .drop("conversion")
      .select(
          "campaign_id",
          "customer_id",
          "date",
          "conversion_type",
          "revenue_usd"
      )         
      .dropDuplicates(["campaign_id", "customer_id", "date", "conversion_type"])   
    )

    df_transpose = (
    df
    .withColumn("add_to_cart", when(col("conversion_type") == 'Add to Cart',1).otherwise(0))
    .withColumn("wishlist", when(col("conversion_type") == 'Wishlist',1).otherwise(0))
    .withColumn("purchase", when(col("conversion_type") == 'Buy',1).otherwise(0))
    .groupBy(["campaign_id", "customer_id", "date"])
    .agg(sum("add_to_cart").alias("add_to_cart"), 
         sum("wishlist").alias("wishlist"), 
         sum("purchase").alias("purchase"),
         sum("revenue_usd").alias("revenue_usd")
    )
    .orderBy("date")
    )
    return df_transpose 
