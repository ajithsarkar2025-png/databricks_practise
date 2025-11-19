from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table(name="customers",comment="clean customers")

dp.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation = 'delete'"),
    except_column_list=["operation_date","_rescued_data","operation"],
    )