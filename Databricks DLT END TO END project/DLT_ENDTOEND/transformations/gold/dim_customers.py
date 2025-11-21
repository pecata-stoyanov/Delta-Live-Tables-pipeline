import dlt
from pyspark.sql.functions import *

#Gold Streaming View On Top of Silver View( Not Silver Table)
@dlt.view(
    name = "customers_gold_view"
)


def customers_gold_view():
    df = spark.readStream.table("customers_silver_view")
    return df


#Creating DIM CDC type 2 Table (With auto cdc)

dlt.create_streaming_table(name="dim_customers")

dlt.create_auto_cdc_flow(
    target = 'dim_customers',
    source = 'customers_gold_view',
    keys =   ['customer_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = '2',
    except_column_list = ['processDate']
    )





