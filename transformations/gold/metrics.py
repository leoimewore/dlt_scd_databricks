import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table
@dlt.expect("positive_total_revenue", "total_revenue >= 0")
@dlt.expect("positive_trip_count", "trip_count > 0")
def daily_metrics():
    fact = dlt.read("facts_payments")

    return (
        fact.groupBy("date_start_key")
            .agg(
                sum("trip_total").alias("total_revenue"),
                count("*").alias("trip_count")
            )
    )
