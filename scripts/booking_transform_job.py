"""
Step 4 orchestrator: wires together JsonDataReader, mapping loading, and
PartitionTransformer to produce a typed, mapped DataFrame from raw booking JSON.

Dependencies (reader, mapping loaders) are injected via the constructor --
this follows the Dependency Inversion Principle and makes the job unit-testable
without needing real files or a real Spark cluster.
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from typing import Callable, List

from scripts.exchange_rate_provider import FrankfurterExchangeRateProvider
from scripts.interfaces import ExchangeRateRepository, MappingRepository

from scripts.interfaces import MappingRepository
from scripts.json_data_reader import JsonDataReader
from scripts.logging_config import configure_logging, get_logger
from scripts.mapping_loader import JsonMappingLoader, get_mappings_dir
from scripts.partition_transformer import PartitionTransformer
from scripts.spark_schema import MAPPED_RECORD_SCHEMA
from scripts.spark_session import create_spark_session

logger = get_logger(__name__)


class BookingTransformJob:
    """Orchestrates reading raw booking JSON and transforming it into the target schema."""

    def __init__(
        self,
        spark: SparkSession,
        reader: JsonDataReader,
        status_mapping_repo: MappingRepository,
        country_region_mapping_repo: MappingRepository,
        exchange_rate_provider_factory: Callable[[List[str]], ExchangeRateRepository] = FrankfurterExchangeRateProvider,
    ):
        self._spark = spark
        self._reader = reader
        self._status_mapping_repo = status_mapping_repo
        self._country_region_mapping_repo = country_region_mapping_repo
        self._exchange_rate_provider_factory = exchange_rate_provider_factory

    def read_raw(self, json_path: str) -> DataFrame:
        return self._reader.read(json_path)

    def _get_distinct_currencies(self, raw_df: DataFrame) -> List[str]:
        rows = raw_df.select("currencies.booker").distinct().collect()
        return [row["booker"] for row in rows if row["booker"] is not None]

    def transform(self, raw_df: DataFrame) -> DataFrame:
        status_mapping = self._status_mapping_repo.load()
        country_region_mapping = self._country_region_mapping_repo.load()

        distinct_currencies = self._get_distinct_currencies(raw_df)
        exchange_rates = self._exchange_rate_provider_factory(distinct_currencies).load()
        logger.info("Distinct currencies found: %s", distinct_currencies)

        status_bc = self._spark.sparkContext.broadcast(status_mapping)
        region_bc = self._spark.sparkContext.broadcast(country_region_mapping)
        rates_bc = self._spark.sparkContext.broadcast(exchange_rates)

        transformer = PartitionTransformer(status_bc.value, region_bc.value, rates_bc.value)
        transformed_rdd = raw_df.rdd.mapPartitions(transformer)

        return self._spark.createDataFrame(transformed_rdd, schema=MAPPED_RECORD_SCHEMA)

    @classmethod
    def build_default(cls, spark: SparkSession) -> "BookingTransformJob":
        mappings_dir = get_mappings_dir()
        return cls(
            spark=spark,
            reader=JsonDataReader(spark),
            status_mapping_repo=JsonMappingLoader(mappings_dir / "status_mapping.json"),
            country_region_mapping_repo=JsonMappingLoader(mappings_dir / "country_region.json"),
        )

def main() -> None:
    configure_logging()
    spark = create_spark_session()

    try:
        job = BookingTransformJob.build_default(spark)

        raw_json_path = str(Path(__file__).resolve().parent.parent / "data" / "bookings_large.jsonl")
        raw_df = job.read_raw(raw_json_path)
        logger.info("Raw record count: %d", raw_df.count())

        # --- Sanity check on a small sample BEFORE running on the full 1GB file ---
        sample_df = raw_df.limit(100)
        mapped_sample_df = job.transform(sample_df)

        logger.info("Sample transformed record count: %d", mapped_sample_df.count())
        mapped_sample_df.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()