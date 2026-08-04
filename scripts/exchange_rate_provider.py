"""
Fetches currency exchange rates (to USD) from the Frankfurter API --
a free, no-API-key-required service backed by European Central Bank data.
Implements the ExchangeRateRepository protocol.
"""
from typing import Dict, Iterable

import requests

from scripts.exceptions import ExchangeRateFetchError
from scripts.logging_config import get_logger

logger = get_logger(__name__)


class FrankfurterExchangeRateProvider:
    """Fetches 'currency -> USD' conversion rates for a given set of currencies."""

    _API_URL = "https://api.frankfurter.dev/v1/latest"

    def __init__(self, currencies: Iterable[str], timeout_seconds: int = 10):
        self._currencies = sorted({c.upper() for c in currencies if c})
        self._timeout_seconds = timeout_seconds

    def load(self) -> Dict[str, float]:
        if not self._currencies:
            return {}

        currencies_to_fetch = [c for c in self._currencies if c != "USD"]
        rates_to_usd: Dict[str, float] = {"USD": 1.0}

        if not currencies_to_fetch:
            return rates_to_usd

        try:
            response = requests.get(
                self._API_URL,
                params={"base": "USD", "symbols": ",".join(currencies_to_fetch)},
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise ExchangeRateFetchError(f"Failed to fetch exchange rates: {exc}") from exc

        # API gives USD -> currency; we need currency -> USD, so invert.
        usd_to_currency_rates = payload.get("rates", {})
        for currency_code, usd_to_currency_rate in usd_to_currency_rates.items():
            if usd_to_currency_rate:
                rates_to_usd[currency_code] = 1.0 / usd_to_currency_rate
            else:
                logger.warning("Zero rate returned for %s; skipping.", currency_code)

        missing = set(currencies_to_fetch) - set(rates_to_usd.keys())
        if missing:
            logger.warning("No exchange rate found for currencies: %s", missing)

        logger.info("Loaded exchange rates for %d currencies.", len(rates_to_usd))
        return rates_to_usd