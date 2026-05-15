from pyspark.sql.functions import *
from pyspark.sql.types import *

def dtypes_function(df):
    #Renaming columns
    df = df.withColumnRenamed('dropoff_centroid_latitude', 'dropoff_lat') \
            .withColumnRenamed('dropoff_centroid_longitude', 'dropoff_long') \
            .withColumnRenamed('pickup_centroid_latitude', 'pickup_lat') \
            .withColumnRenamed('pickup_centroid_longitude', 'pickup_long')


    df = df.withColumn('dropoff_lat', col('dropoff_lat').cast(DoubleType())) \
            .withColumn('dropoff_long', col('dropoff_long').cast(DoubleType())) \
            .withColumn('extras', col('extras').cast(DoubleType())) \
            .withColumn('fare',col('fare').cast(DoubleType())) \
            .withColumn('pickup_lat', col('pickup_lat').cast(DoubleType())) \
            .withColumn('pickup_long', col('pickup_long').cast(DoubleType())) \
            .withColumn('tips', col('tips').cast(DoubleType())) \
            .withColumn('tolls',col('tolls').cast(DoubleType())) \
            .withColumn('trip_end_timestamp', col('trip_end_timestamp').cast(TimestampType())) \
            .withColumn('trip_start_timestamp', col('trip_start_timestamp').cast(TimestampType())) \
            .withColumn('trip_miles', col('trip_miles').cast(DoubleType())) \
            .withColumn('trip_seconds',col('trip_seconds').cast(IntegerType())) \
            .withColumn('trip_total',col('trip_total').cast(DoubleType())) 
    return df

     

   

