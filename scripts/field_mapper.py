"""
Transforms a single raw booking record into the target MappedRecord schema.
Depends on the MappingRepository / ExchangeRateRepository abstractions
(Dependency Inversion Principle), not on any specific loading mechanism.
"""
from typing import Any, Dict, Optional

from scripts.exceptions import RecordTransformError
from scripts.interfaces import ExchangeRateRepository, MappingRepository
from scripts.label_parser import LabelParser
from scripts.logging_config import get_logger
from scripts.schema import MappedRecord

logger = get_logger(__name__)


class FieldMapper:
    """Maps a raw source record dict into the flat MappedRecord target schema."""

    def __init__(
        self,
        status_mapping: MappingRepository,
        country_region_mapping: MappingRepository,
        exchange_rates: ExchangeRateRepository,
    ):
        self._status_mapping = status_mapping.load()
        self._country_region_mapping = country_region_mapping.load()
        self._exchange_rates = exchange_rates.load()

    def map_record(self, record: Dict[str, Any]) -> MappedRecord:
        try:
            label_parts = LabelParser.parse(record.get("label"))
            country_code = self._extract_country_code(record)
            raw_status = record.get("status")
            currency = self._safe_get(record, "currencies", "booker")
            revenue = 0.0  # hotels are not mapped to commission revenue

            return MappedRecord(
                transaction_id=self._safe_get(record, "accommodations", "reservation"),
                conversion_key=record.get("label"),
                site_key=label_parts["site_key"],
                device=label_parts["device"],
                referral_property_id=label_parts["referral_property_id"],
                property_id=self._build_property_id(record),
                status=self._status_mapping.get(raw_status, raw_status),
                travel_purpose=self._safe_get(record, "booker", "travel_purpose"),
                country_code=country_code,
                region=self._country_region_mapping.get(country_code, "other"),
                currency=currency,
                check_in_date=self._date_only(record.get("start")),
                check_out_date=self._date_only(record.get("end")),
                revenue=revenue,
                revenue_usd=self._convert_to_usd(revenue, currency),
            )
        except Exception as exc:
            record_id = record.get("id", "UNKNOWN")
            raise RecordTransformError(
                f"Failed to transform record id={record_id}: {exc}"
            ) from exc

    def _convert_to_usd(self, amount: float, currency: Optional[str]) -> Optional[float]:
        if currency is None:
            return None
        rate = self._exchange_rates.get(currency.upper())
        if rate is None:
            logger.warning("No exchange rate available for currency: %s", currency)
            return None
        return round(amount * rate, 2)

    def _build_property_id(self, record: Dict[str, Any]) -> Optional[str]:
        accommodation_id = self._safe_get(record, "accommodation_details", "accommodation")
        return f"BC-{accommodation_id}" if accommodation_id is not None else None

    def _extract_country_code(self, record: Dict[str, Any]) -> Optional[str]:
        country = self._safe_get(record, "booker", "address", "country")
        return country.upper() if country else None

    @staticmethod
    def _date_only(timestamp: Optional[str]) -> Optional[str]:
        return timestamp.split("T")[0] if timestamp else None

    @staticmethod
    def _safe_get(record: Dict[str, Any], *keys: str) -> Any:
        """Safely walks nested dict keys; returns None if any key is missing along the path."""
        current: Any = record
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
        return current