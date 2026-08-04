"""
One-time preprocessing script: converts the single large JSON array file
into JSON Lines (.jsonl) format.

Why this is needed: Spark's multiLine JSON reader parses the entire array
in a single task with no splitting, which risks loading the whole 1GB file
into a single JVM's memory at once. JSON Lines, by contrast, is naturally
splittable across partitions, so Spark can read it in parallel with a
much smaller memory footprint per task.

Uses ijson (a streaming parser) so this script itself never holds the
full 1GB file in memory -- it reads and writes one record at a time.
"""
import json
from decimal import Decimal
from pathlib import Path

import ijson

from scripts.logging_config import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


class JsonArrayToJsonLinesConverter:
    """Streams a large top-level JSON array file into JSON Lines format."""

    def __init__(self, source_path: Path, destination_path: Path):
        self._source_path = source_path
        self._destination_path = destination_path

    @staticmethod
    def _json_default(obj):
        """
        ijson yields Decimal for numeric values (to preserve precision while
        streaming). Standard json.dumps cannot serialize Decimal, so we convert
        it here: whole numbers become int, fractional values become float.
        """
        if isinstance(obj, Decimal):
            if obj == obj.to_integral_value():
                return int(obj)
            return float(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def convert(self) -> int:
        record_count = 0
        with open(self._source_path, "rb") as source_file, \
             open(self._destination_path, "w", encoding="utf-8") as dest_file:

            for record in ijson.items(source_file, "item"):
                dest_file.write(json.dumps(record, default=self._json_default) + "\n")
                record_count += 1
                if record_count % 50_000 == 0:
                    logger.info("Converted %d records so far...", record_count)

        logger.info("Done. Total records converted: %d", record_count)
        return record_count


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    source = project_root / "data" / "bookings_large.json"
    destination = project_root / "data" / "bookings_large.jsonl"

    converter = JsonArrayToJsonLinesConverter(source, destination)
    converter.convert()