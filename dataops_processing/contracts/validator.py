"""ST14 Spark-side data contract validation."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from enum import Enum


class ContractViolation(str, Enum):
    """Stable, auditable ST14 violation reason codes."""

    INVALID_PRICE = "invalid_price"
    INVALID_QUANTITY = "invalid_quantity"
    INVALID_SIDE = "invalid_side"
    TIMESTAMP_CLOCK_DRIFT = "timestamp_clock_drift"
    INVALID_SYMBOL = "invalid_symbol"
    MISSING_REQUIRED_FIELD = "missing_required_field"


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating one canonical trade row."""

    is_valid: bool
    violations: tuple[ContractViolation, ...]

    def reason_codes(self) -> tuple[str, ...]:
        """Return stable string reason codes for logging and audit."""
        return tuple(violation.value for violation in self.violations)


def validate_trade(
    *,
    symbol: str | None,
    price: float | None,
    quantity: float | None,
    side: str | None,
    event_time_ms: int | None,
    max_clock_drift_seconds: int = 5,
) -> ValidationResult:
    """
    Validate a trade against the ST14 zero-trust data contract.

    Args:
        symbol: Canonical instrument symbol.
        price: Upstream trade price.
        quantity: Upstream trade quantity.
        side: Canonical trade side. Only ``buy`` and ``sell`` are accepted.
        event_time_ms: Upstream event time in Unix milliseconds.
        max_clock_drift_seconds: Maximum absolute clock drift allowed.

    Returns:
        A :class:`ValidationResult`; ``is_valid`` is false when any violation
        is found, and ``violations`` contains stable reason codes.
    """
    violations: list[ContractViolation] = []

    if not symbol or not str(symbol).strip():
        violations.append(ContractViolation.INVALID_SYMBOL)

    if price is None:
        violations.append(ContractViolation.MISSING_REQUIRED_FIELD)
    else:
        try:
            price_value = float(price)
            if not math.isfinite(price_value) or price_value <= 0:
                violations.append(ContractViolation.INVALID_PRICE)
        except (TypeError, ValueError, OverflowError):
            violations.append(ContractViolation.INVALID_PRICE)

    if quantity is None:
        violations.append(ContractViolation.MISSING_REQUIRED_FIELD)
    else:
        try:
            quantity_value = float(quantity)
            if not math.isfinite(quantity_value) or quantity_value <= 0:
                violations.append(ContractViolation.INVALID_QUANTITY)
        except (TypeError, ValueError, OverflowError):
            violations.append(ContractViolation.INVALID_QUANTITY)

    if side not in {"buy", "sell"}:
        violations.append(ContractViolation.INVALID_SIDE)

    if event_time_ms is None:
        violations.append(ContractViolation.MISSING_REQUIRED_FIELD)
    else:
        try:
            event_ms = int(event_time_ms)
            if event_ms <= 0:
                violations.append(ContractViolation.MISSING_REQUIRED_FIELD)
            else:
                system_ms = int(time.time() * 1000)
                drift_seconds = abs(system_ms - event_ms) / 1000
                if drift_seconds > max_clock_drift_seconds:
                    violations.append(
                        ContractViolation.TIMESTAMP_CLOCK_DRIFT
                    )
        except (TypeError, ValueError, OverflowError):
            violations.append(ContractViolation.MISSING_REQUIRED_FIELD)

    unique_violations = tuple(dict.fromkeys(violations))
    return ValidationResult(
        is_valid=len(unique_violations) == 0,
        violations=unique_violations,
    )
