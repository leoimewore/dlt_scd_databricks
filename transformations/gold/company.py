import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "dim_company"
)
@dlt.expect_or_drop("company_not_null","company is not null")
def func_two():
    df = spark.read.table('silver_trips')
    df_company = df.select('company').distinct().withColumn('dim_company_key', monotonically_increasing_id())
    return df_company