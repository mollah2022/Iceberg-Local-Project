"""
Typed contract for the output record shape.
Using TypedDict gives static type-checking (mypy/IDE) on field names,
catching typos like 'trasaction_id' at development time instead of runtime.
"""
from typing import TypedDict, Optional


class MappedRecord(TypedDict):
    transaction_id: Optional[str]
    conversion_key: Optional[str]
    site_key: Optional[str]
    device: Optional[str]
    referral_property_id: Optional[str]
    property_id: Optional[str]
    status: Optional[str]
    travel_purpose: Optional[str]
    country_code: Optional[str]
    region: str
    currency: Optional[str]
    check_in_date: Optional[str]
    check_out_date: Optional[str]
    revenue: float
    revenue_usd: Optional[float]