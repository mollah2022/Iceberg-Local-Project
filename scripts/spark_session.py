"""
Spark session factory for local Iceberg setup.
Single Responsibility: only responsible for creating and configuring the Spark session.
"""
from pyspark.sql import SparkSession
from pathlib import Path


ICEBERG_VERSION = "1.5.2"
SPARK_ICEBERG_PACKAGE = f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}"
CATALOG_NAME = "local"


def get_warehouse_path() -> str:
    """Returns the absolute path of the warehouse/ folder inside the project root."""
    project_root = Path(__file__).resolve().parent.parent
    warehouse_dir = project_root / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    return f"file://{warehouse_dir}"


def create_spark_session(app_name: str = "IcebergLocalPipeline") -> SparkSession:
    """Creates a SparkSession configured with Iceberg extensions and a local Hadoop catalog."""
    warehouse_path = get_warehouse_path()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", SPARK_ICEBERG_PACKAGE)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse_path)
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .config("spark.driver.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
    )

    return builder.getOrCreate()