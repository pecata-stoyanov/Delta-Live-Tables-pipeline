import dlt
from pyspark.sql.functions import *

#Gold Streaming View On Top of Silver View( Not Silver Table)
@dlt.view(
    name = "sales_gold_view"
)


def sales_gold_view():
    df = spark.readStream.table("sales_silver_view")
    return df


#Creating Fact Table (With auto cdc)

dlt.create_streaming_table(name="fact_sales")

dlt.create_auto_cdc_flow(
    target = 'fact_sales',
    source = 'sales_gold_view',
    keys =   ['sales_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = '1')