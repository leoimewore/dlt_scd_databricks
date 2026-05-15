import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.join import join_func


@dlt.table(
    name = "facts_trips"
)
@dlt.expect_or_drop("trip_id_not_null","trip_id is not null")
# Check for orphan keys
@dlt.expect_or_drop("valid_date_start_key", "date_start_key IS NOT NULL")
@dlt.expect_or_drop("valid_date_end_key", "date_end_key IS NOT NULL")
@dlt.expect_or_drop("valid_time_start_key", "time_start_key IS NOT NULL")
@dlt.expect_or_drop("valid_time_end_key", "time_end_key IS NOT NULL")
@dlt.expect("positive_duration", "trip_seconds > 0")
def trips():
    df = spark.read.table('silver_trips')
    df_payment_type = spark.read.table('dim_payments')
    df_company = spark.read.table('dim_company')
    df_date = spark.read.table('dim_date')
    df_time =spark.read.table('dim_time')
                
    facts_trips = join_func(df,df_payment_type,df_company,df_date,df_time)
    facts_trips = facts_trips.select('dim_payment_key','dim_company_key','date_start_key','date_end_key','time_start_key','time_end_key','trip_id','trip_miles','trip_seconds')

    

    return facts_trips
    