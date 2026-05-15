import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "bronze_trips"
)
@dlt.expect("trip_id_null_count","trip_id is not null")
@dlt.expect("location_info_null_count", "dropoff_centroid_latitude is not null")
def func_one():
    df = spark.readStream.table('dlt_taxi_trips.raw.raw_trips')
    return df



    
                              
