"""
Responsible only for transforming the rows of a single Spark partition.
Designed as a callable class (not a closure/lambda) so it is picklable and
testable in isolation -- this is the standard pattern for mapPartitions logic.
"""
from typing import Any, Dict, Iterator

from pyspark.sql import Row

from scripts.exceptions import RecordTransformError
from scripts.field_mapper import FieldMapper
from scripts.logging_config import get_logger
from scripts.static_mapping_repository import StaticMappingRepository, StaticExchangeRateRepository

logger = get_logger(__name__)


class PartitionTransformer:
    """
    Callable that transforms one partition's worth of raw rows into mapped Rows.
    A single FieldMapper instance is built once per partition (in __call__),
    and reused for every row in that partition -- this is the main efficiency
    win over a row-level UDF, which would rebuild state per row.
    """

    def __init__(
        self,
        status_mapping: Dict[str, str],
        country_region_mapping: Dict[str, str],
        exchange_rates: Dict[str, float],
    ):
        self._status_mapping = status_mapping
        self._country_region_mapping = country_region_mapping
        self._exchange_rates = exchange_rates

    def __call__(self, rows: Iterator[Row]) -> Iterator[Row]:
        mapper = FieldMapper(
            StaticMappingRepository(self._status_mapping),
            StaticMappingRepository(self._country_region_mapping),
            StaticExchangeRateRepository(self._exchange_rates),
        )
        for row in rows:
            record: Dict[str, Any] = row.asDict(recursive=True)
            try:
                mapped = mapper.map_record(record)
                yield Row(**mapped)
            except RecordTransformError as exc:
                logger.warning("Skipping unmappable record: %s", exc)
                continue