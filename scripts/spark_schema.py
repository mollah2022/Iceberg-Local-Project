"""
Explicit output schema for the mapped/transformed records.
Defining this explicitly (instead of letting Spark infer it) avoids
schema-drift bugs and makes the contract visible in one place.
"""
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

MAPPED_RECORD_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("conversion_key", StringType(), True),
        StructField("site_key", StringType(), True),
        StructField("device", StringType(), True),
        StructField("referral_property_id", StringType(), True),
        StructField("property_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("travel_purpose", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("region", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("check_in_date", StringType(), True),
        StructField("check_out_date", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("revenue_usd", DoubleType(), True),
    ]
)