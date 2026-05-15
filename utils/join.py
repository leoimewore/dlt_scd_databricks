from pyspark.sql.functions import *
from pyspark.sql.types import *

def join_func(df,df_payment_type,df_company,df_date,df_time):
    df = df.withColumn('date_start_key',date_format(col('trip_start_timestamp'),'yyyyMMdd'))\
        .withColumn('date_end_key',date_format(col('trip_end_timestamp'),'yyyyMMdd')) \
        .withColumn('time_start_key',date_format(col('trip_start_timestamp'),"HHmmss"))\
        .withColumn('time_end_key',date_format(col('trip_end_timestamp'),"HHmmss"))
  
    
    

    df = df.join(df_payment_type, df.payment_type == df_payment_type.payment_type, "left") \
                .join(df_company, df.company == df_company.company, "left") \
                .join(df_date.alias('start'), df.date_start_key == col("start.date_key"), "left") \
                .join(df_date.alias('end'), df.date_end_key == col("end.date_key"), "left") \
                .join(df_time.alias('time_start'), df.time_start_key == col("time_start.datetime_key"), "left") \
                .join(df_time.alias('time_end'), df.time_end_key == col("time_end.datetime_key"), "left")


    return df