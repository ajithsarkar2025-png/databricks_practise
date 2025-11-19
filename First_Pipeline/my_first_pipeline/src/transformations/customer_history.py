from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table(name="customers_history",comment="maintain customer history")

dp.create_auto_cdc_flow(

    target="customers_history",
    source="customers_cdc_clean",
    keys=["id"],
    sequence_by=col("operation_date"),
    ignore_null_updates=False,
    apply_as_deletes=expr("operation='DELETE'"),
    except_column_list=["operation_date","operation","_rescued_data"],
    stored_as_scd_type="2"



)