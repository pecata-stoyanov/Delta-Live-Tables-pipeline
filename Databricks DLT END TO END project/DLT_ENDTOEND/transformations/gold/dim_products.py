import dlt
from pyspark.sql.functions import *

#Gold Streaming View On Top of Silver View( Not Silver Table)
@dlt.view(
    name = "products_gold_view"
)


def products_gold_view():
    df = spark.readStream.table("products_silver_view")
    return df


#Creating DIM CDC type 2 Table (With auto cdc)

dlt.create_streaming_table(name="dim_products")

dlt.create_auto_cdc_flow(
    target = 'dim_products',
    source = 'products_gold_view',
    keys =   ['product_id'],
    sequence_by = col('processDate'),
    stored_as_scd_type = '2',
    except_column_list = ['processDate']
    )





