"""
Step 6 orchestrator: runs the full pipeline end-to-end.
Reads the entire JSON Lines file, transforms every record, and writes
the result into the local Iceberg table.
"""
from pathlib import Path

from pyspark.sql import SparkSession

from scripts.booking_transform_job import BookingTransformJob
from scripts.iceberg_table_writer import IcebergTableWriter
from scripts.logging_config import configure_logging, get_logger
from scripts.spark_session import create_spark_session

logger = get_logger(__name__)


class FullPipelineRunner:
    """
    Orchestrates the end-to-end pipeline: read raw JSONL -> transform ->
    write to the local Iceberg table. Owns the SparkSession lifecycle.
    """

    def __init__(self, spark: SparkSession, raw_json_path: str):
        self._spark = spark
        self._raw_json_path = raw_json_path
        self._job = BookingTransformJob.build_default(spark)
        self._writer = IcebergTableWriter(spark)

    def run(self) -> None:
        raw_df = self._job.read_raw(self._raw_json_path)
        logger.info("Raw record count: %d", raw_df.count())

        mapped_df = self._job.transform(raw_df)
        self._writer.write(mapped_df, mode="overwrite")

        logger.info("Pipeline completed successfully.")

    def shutdown(self) -> None:
        self._spark.stop()


def main() -> None:
    configure_logging()
    spark = create_spark_session()

    raw_json_path = str(Path(__file__).resolve().parent.parent / "data" / "bookings_large.jsonl")
    runner = FullPipelineRunner(spark, raw_json_path)

    try:
        runner.run()
    finally:
        runner.shutdown()


if __name__ == "__main__":
    main()