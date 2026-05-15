import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

@dlt.table(
    name = "dim_time"
)
def dim_time():
    start = datetime.strptime("00:00:00", "%H:%M:%S")
    end = datetime.strptime("23:59:59", "%H:%M:%S")

    # Generate list of time strings
    times = []
    current = start
    while current <= end:
        times.append((current.strftime("%H:%M:%S"),))
        current += timedelta(seconds=1)

    # Create DataFrame with STRING column
    df = spark.createDataFrame(times, ["time_str"])

    # Convert to TIMESTAMP for extraction
    df = df.withColumn("time_ts", to_timestamp(col("time_str"), "HH:mm:ss"))

    return (
        df
        .withColumn("datetime_key", date_format(col("time_ts"), "HHmmss").cast("int"))
        .withColumn("hour", hour("time_ts"))
        .withColumn("minute", minute("time_ts"))
        .withColumn("second", second("time_ts"))
        .drop("time_ts")
    )
    

    
