"""Tests for the ST14 Spark-side data contract validator."""

import time
import unittest

from dataops_processing.contracts.validator import (
    ContractViolation,
    validate_trade,
)


class TestST14ContractValidator(unittest.TestCase):
    """Verify invalid upstream trades are rejected before VWAP aggregation."""

    def test_rejects_negative_price(self) -> None:
        """Negative prices must produce invalid_price."""
        result = validate_trade(
            symbol="BTCUSDT",
            price=-1,
            quantity=1,
            side="buy",
            event_time_ms=int(time.time() * 1000),
        )
        self.assertFalse(result.is_valid)
        self.assertIn(ContractViolation.INVALID_PRICE, result.violations)

    def test_rejects_zero_quantity(self) -> None:
        """Zero quantity must not reach VWAP aggregation."""
        result = validate_trade(
            symbol="BTCUSDT",
            price=65000,
            quantity=0,
            side="buy",
            event_time_ms=int(time.time() * 1000),
        )
        self.assertFalse(result.is_valid)
        self.assertIn(ContractViolation.INVALID_QUANTITY, result.violations)

    def test_rejects_invalid_side(self) -> None:
        """A side outside buy/sell must be quarantined."""
        result = validate_trade(
            symbol="BTCUSDT",
            price=65000,
            quantity=0.01,
            side="unknown",
            event_time_ms=int(time.time() * 1000),
        )
        self.assertFalse(result.is_valid)
        self.assertIn(ContractViolation.INVALID_SIDE, result.violations)

    def test_rejects_clock_drift(self) -> None:
        """Stale or future events beyond five seconds must be rejected."""
        result = validate_trade(
            symbol="BTCUSDT",
            price=65000,
            quantity=0.01,
            side="buy",
            event_time_ms=int(time.time() * 1000) - 10_000,
            max_clock_drift_seconds=5,
        )
        self.assertFalse(result.is_valid)
        self.assertIn(
            ContractViolation.TIMESTAMP_CLOCK_DRIFT,
            result.violations,
        )

    def test_accepts_valid_trade(self) -> None:
        """A valid trade must pass the ST14 guard."""
        result = validate_trade(
            symbol="BTCUSDT",
            price=65000,
            quantity=0.01,
            side="buy",
            event_time_ms=int(time.time() * 1000),
        )
        self.assertTrue(result.is_valid)
        self.assertEqual(result.violations, ())
