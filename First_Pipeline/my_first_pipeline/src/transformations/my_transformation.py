from pyspark import pipelines as dp
from pyspark.sql.functions import *

catalog = "practise"
schema = "practiseSchema"

path = f"/Volumes/{catalog}/{schema}/raw_data/customers"

dp.create_streaming_table("customer_cdc_bronze", comment="Customer cdc bronze table")
@dp.append_flow(target="customer_cdc_bronze", name="customers_bronze_ingest_flow")
def customers_bronze_ingest_flow():
    return(spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .option("cloudFiles.inferColumnTypes", "true")\
        .load(f"{path}")
           )
