"""
Spark session factory for local Iceberg setup.
Single Responsibility: only responsible for creating and configuring the Spark session.
"""
from pathlib import Path

from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Builds a SparkSession configured with Iceberg extensions and a local Hadoop catalog."""

    ICEBERG_VERSION = "1.5.2"
    CATALOG_NAME = "local"
    SPARK_MASTER = "local[3]"  # explicit core count instead of local[*] -- see note below

    def __init__(self, app_name: str = "IcebergLocalPipeline"):
        self._app_name = app_name

    @property
    def _spark_iceberg_package(self) -> str:
        return f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{self.ICEBERG_VERSION}"

    def _get_warehouse_path(self) -> str:
        """Returns the absolute path of the warehouse/ folder inside the project root."""
        project_root = Path(__file__).resolve().parent.parent
        warehouse_dir = project_root / "warehouse"
        warehouse_dir.mkdir(parents=True, exist_ok=True)
        return f"file://{warehouse_dir}"

    def create(self) -> SparkSession:
        """Builds and returns a configured SparkSession."""
        warehouse_path = self._get_warehouse_path()

        builder = (
            SparkSession.builder.appName(self._app_name)
            .master(self.SPARK_MASTER)
            .config("spark.jars.packages", self._spark_iceberg_package)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{self.CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{self.CATALOG_NAME}.type", "hadoop")
            .config(f"spark.sql.catalog.{self.CATALOG_NAME}.warehouse", warehouse_path)
            .config("spark.sql.defaultCatalog", self.CATALOG_NAME)
            .config("spark.driver.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.sql.shuffle.partitions", "8")
        )

        return builder.getOrCreate()


def create_spark_session(app_name: str = "IcebergLocalPipeline") -> SparkSession:
    """
    Convenience wrapper so existing call-sites (create_spark_session())
    across the project keep working without needing to change every import.
    """
    return SparkSessionFactory(app_name).create()