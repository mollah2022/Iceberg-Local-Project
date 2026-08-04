"""
A MappingRepository implementation that wraps an in-memory dict.
Used to feed broadcast mapping values into FieldMapper on Spark executors,
where reading from the local filesystem again would be wasteful/unsafe.
"""
from typing import Dict


class StaticMappingRepository:
    """Wraps a plain dict so it satisfies the MappingRepository protocol."""

    def __init__(self, data: Dict[str, str]):
        self._data = data

    def load(self) -> Dict[str, str]:
        return self._data



class StaticExchangeRateRepository:
    """Wraps a plain dict so it satisfies the ExchangeRateRepository protocol."""

    def __init__(self, data: Dict[str, float]):
        self._data = data

    def load(self) -> Dict[str, float]:
        return self._data