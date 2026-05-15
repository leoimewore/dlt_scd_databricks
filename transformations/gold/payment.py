import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "dim_payments"
)
@dlt.expect_or_drop("payment_type_not_null","payment_type is not null")
def func_one():
    df = spark.read.table('silver_trips')
    df_payment = df.select('payment_type').distinct().withColumn('dim_payment_key', monotonically_increasing_id())
    return df_payment
