from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="customer_update_summary",
    comment="Table represents update summary"
)
def customer_update_summary():
    return(

        spark.read.table("customers_history").groupBy("id").agg(
            count("firstname").alias("first_name_count"),
            count("lastname").alias("last_name_count"),
            count("address").alias("address_count"),
            count("email").alias("email_count"),
            
        )

    )