def nullhandling(df):
    numeric_cols = [c for c, t in df.dtypes if t in ("int", "double")]
    df = df.fillna(0, subset=numeric_cols)

    string_cols = [c for c,t in df.dtypes if t in ("string")]
    df = df.fillna('UNKNOWN',subset=string_cols)

    avg_trip_time = df.select(round(avg(col("trip_seconds")/60),0).alias("avg_trip_seconds")).collect()[0][0] 

    df = df.withColumn('trip_end_timestamp',when(
        col('trip_end_timestamp').isNull(),expr(f"trip_start_timestamp + INTERVAL  {int(avg_trip_time)} MINUTES")).otherwise(col('trip_end_timestamp')))


    return df