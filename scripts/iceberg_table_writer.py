"""
Responsible only for creating (if needed) and writing into the target
local Iceberg table. Single Responsibility: table DDL + write operations.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date

from scripts.logging_config import get_logger

logger = get_logger(__name__)


class IcebergTableWriter:
    """
    Creates (if needed) and writes into a local Iceberg table.
    Partitioned by month of check_in_date + region for fast date-range
    and region-scoped queries (partition pruning).
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "local",
        database: str = "db",
        table: str = "bookings",
    ):
        self._spark = spark
        self._database_fqn = f"{catalog}.{database}"
        self._table_fqn = f"{catalog}.{database}.{table}"

    def _prepare_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Casts the date-like string columns (e.g. '2026-09-15') into proper
        DATE type. This is required for Iceberg's months() partition
        transform to work -- it cannot partition on a plain string.
        """
        return (
            df.withColumn("check_in_date", to_date(col("check_in_date")))
            .withColumn("check_out_date", to_date(col("check_out_date")))
        )

    def _create_table_if_not_exists(self) -> None:
        self._spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {self._database_fqn}")
        self._spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table_fqn} (
                transaction_id STRING,
                conversion_key STRING,
                site_key STRING,
                device STRING,
                referral_property_id STRING,
                property_id STRING,
                status STRING,
                travel_purpose STRING,
                country_code STRING,
                region STRING,
                currency STRING,
                check_in_date DATE,
                check_out_date DATE,
                revenue DOUBLE,
                revenue_usd DOUBLE
            )
            USING iceberg
            PARTITIONED BY (months(check_in_date), region)
            """
        )
        logger.info("Ensured Iceberg table exists: %s", self._table_fqn)

    def write(self, df: DataFrame, mode: str = "overwrite") -> None:
        """
        mode='overwrite' -> replaces all existing data (safe for repeated
        local dev runs -- no risk of duplicate rows from re-running).
        mode='append'    -> adds new rows on top of existing data
        (use this once you're doing incremental/production loads).
        """
        self._create_table_if_not_exists()
        prepared_df = self._prepare_dataframe(df)
        prepared_df.createOrReplaceTempView("_incoming_records")

        if mode == "overwrite":
            self._spark.sql(f"INSERT OVERWRITE {self._table_fqn} SELECT * FROM _incoming_records")
        elif mode == "append":
            self._spark.sql(f"INSERT INTO {self._table_fqn} SELECT * FROM _incoming_records")
        else:
            raise ValueError(f"Unsupported write mode: {mode}")

        record_count = df.count()
        logger.info("Wrote %d records into %s (mode=%s)", record_count, self._table_fqn, mode)