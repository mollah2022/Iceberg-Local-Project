"""
Parses the underscore-separated 'label' field into structured attribution data.
Single Responsibility: label string parsing only.

Example label: k-htl_d-m_u-....._p-BC-483720
"""
from typing import Dict, Optional
from scripts.logging_config import get_logger

logger = get_logger(__name__)

_DEVICE_CODE_MAP: Dict[str, str] = {
    "m": "mobile",
    "d": "desktop",
    "t": "tablet",
}


class LabelParser:
    """Stateless parser for the 'label' attribution string."""

    @staticmethod
    def parse(label: Optional[str]) -> Dict[str, Optional[str]]:
        result: Dict[str, Optional[str]] = {
            "site_key": None,
            "device": None,
            "referral_property_id": None,
        }

        if not label:
            logger.debug("Empty or missing label; returning null attribution fields.")
            return result

        for segment in label.split("_"):
            if "-" not in segment:
                continue
            prefix, _, value = segment.partition("-")

            if prefix == "k":
                result["site_key"] = value.upper()
            elif prefix == "d":
                result["device"] = _DEVICE_CODE_MAP.get(value, value)
            elif prefix == "p":
                result["referral_property_id"] = value

        return result