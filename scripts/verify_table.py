"""
Step 7: Read-only verification for the local Iceberg table.
Confirms row count, schema, sample rows, and partition distribution.
"""
from pyspark.sql import SparkSession

from scripts.logging_config import configure_logging, get_logger
from scripts.spark_session import create_spark_session

logger = get_logger(__name__)


class TableVerifier:
    """Runs read-only sanity checks against a given Iceberg table."""

    def __init__(self, spark: SparkSession, table_fqn: str):
        self._spark = spark
        self._table_fqn = table_fqn

    def print_row_count(self) -> int:
        count = self._spark.sql(f"SELECT COUNT(*) AS cnt FROM {self._table_fqn}").collect()[0]["cnt"]
        logger.info("Total rows in table: %d", count)
        return count

    def print_schema(self) -> None:
        print("\n=== Schema ===")
        self._spark.sql(f"DESCRIBE {self._table_fqn}").show(50, truncate=False)

    def print_sample_rows(self, limit: int = 5) -> None:
        print("\n=== Sample rows ===")
        self._spark.sql(f"SELECT * FROM {self._table_fqn} LIMIT {limit}").show(truncate=False)

    def print_breakdown_by(self, column: str) -> None:
        print(f"\n=== Row count by {column} ===")
        self._spark.sql(f"""
            SELECT {column}, COUNT(*) AS bookings
            FROM {self._table_fqn}
            GROUP BY {column}
            ORDER BY bookings DESC
        """).show(20, truncate=False)

    def print_snapshots(self) -> None:
        print("\n=== Iceberg snapshots (history) ===")
        self._spark.sql(f"SELECT * FROM {self._table_fqn}.snapshots").show(truncate=False)

    def run_all_checks(self) -> None:
        """Runs the full standard verification suite."""
        self.print_row_count()
        self.print_schema()
        self.print_sample_rows()
        self.print_breakdown_by("region")
        self.print_breakdown_by("status")
        self.print_snapshots()


def main() -> None:
    configure_logging()
    spark = create_spark_session()

    try:
        verifier = TableVerifier(spark, table_fqn="local.db.bookings")
        verifier.run_all_checks()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()