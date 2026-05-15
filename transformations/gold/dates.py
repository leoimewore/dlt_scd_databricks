import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, timedelta

@dlt.table(
    name="dim_date",
    comment="Full date dimension with calendar and fiscal attributes"
)
def dim_date():
    df = spark.read.table("silver_trips")

    # Extract min/max dates from your timestamps
    bounds = df.select(
        to_date(min("trip_start_timestamp")).alias("min_date"),
        to_date(max("trip_end_timestamp")).alias("max_date")
    ).first()

    min_date = bounds.min_date
    max_date = bounds.max_date

    if min_date is None:
        min_date = date.today() - timedelta(days=365)

    if max_date is None:
        max_date = date.today()



    # Generate date sequence
    date_df = spark.createDataFrame(
        [(min_date, max_date)], ["start", "end"]
    ).selectExpr("sequence(start, end, interval 1 day) as date_seq") \
     .select(explode("date_seq").alias("date"))

    # Build full date dimension
    dim = (
        date_df
        .withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("int"))
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", date_format("date", "E"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("is_weekend", col("day_of_week").isin("Sat", "Sun"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("quarter_name", concat(lit("Q"), quarter("date")))
        
    )

    return dim

    