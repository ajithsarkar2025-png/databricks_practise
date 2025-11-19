from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table(name="customers_cdc_clean",
                          expect_all_or_drop={"no_rescued_data":"_rescued_data IS NULL","no_null_id":"id IS NOT NULL","valid_operations": "operation IN('APPEND','UPDATE','DELETE')"})

@dp.append_flow(
    target="customers_cdc_clean",
    name="customers_cdc_clean_flow"
)
def customers_cdc_clean_flow():
    return (
        spark.readStream.table("customer_cdc_bronze")
        .select("address","email","firstname","id","lastname","operation","operation_date","_rescued_data")
        
    )