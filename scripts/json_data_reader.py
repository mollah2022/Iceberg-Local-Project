"""
Responsible only for reading the raw source JSON file into a Spark DataFrame.
Single Responsibility: data reading, no business transformation here.
"""
from pyspark.sql import DataFrame, SparkSession

from scripts.logging_config import get_logger

logger = get_logger(__name__)


class JsonDataReader:
    """Reads a JSON array file and repartitions it for parallel processing."""

    def __init__(self, spark: SparkSession, multi_line: bool = False):
        self._spark = spark
        self._multi_line = multi_line

    def read(self, json_path: str) -> DataFrame:
        """
        multiLine=True is required because the source is a single top-level
        JSON array `[ {...}, {...} ]`, not JSON Lines. This makes the initial
        read less parallel, so we repartition right after loading.
        """
        logger.info("Reading raw JSON from: %s", json_path)
        df = self._spark.read.option("multiLine", str(self._multi_line).lower()).json(json_path)

        target_partitions = self._spark.sparkContext.defaultParallelism
        return df.repartition(target_partitions)