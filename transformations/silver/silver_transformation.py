import dlt
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


def nullhandling(df):
    numeric_cols = [c for c, t in df.dtypes if t in ("int", "double")]
    df = df.fillna(0, subset=numeric_cols)

    string_cols = [c for c, t in df.dtypes if t in ("string")]
    df = df.fillna('UNKNOWN', subset=string_cols)

    return df


dlt.create_streaming_table(
    name = "silver_trips",
    expect_all_or_drop = {"trip_id_null_count": "trip_id IS NOT NULL"}
   
)
@dlt.append_flow(target="silver_trips")
def silver():
    df = spark.readStream.table("bronze_trips")
    df = dtypes_function(df)
    df = nullhandling(df)
    return df


# @dlt.table()
# def avg_trip_duration():
#     df = dlt.read("silver_trips")
#     avg_trip_df = df.select(round(avg(col("trip_seconds")/60),0).alias("avg_trip_seconds"))

#     return avg_trip_df

#     # df = df.withColumn("trip_end_timestamp",when(col("trip_end_timestamp").isNull(),
#     # expr("trip_start_timestamp + INTERVAL avg_trip_minutes MINUTES")
#     # ).otherwise(col("trip_end_timestamp")))
#     # return
    