"""
This script only checks whether Spark + Iceberg local catalog is working correctly.
A sanity check before touching the actual 1GB JSON file.
"""
from spark_session import create_spark_session


def run_sanity_check() -> None:
    spark = create_spark_session()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.test_table (
            id INT,
            name STRING
        ) USING iceberg
    """)
    spark.sql("INSERT INTO local.db.test_table VALUES (1, 'hello'), (2, 'iceberg')")

    print("=== Data from Iceberg table ===")
    spark.sql("SELECT * FROM local.db.test_table").show()

    spark.stop()


if __name__ == "__main__":
    run_sanity_check()