"""
Calculation type handlers — one module per handler family.

Each handler takes (section, request, engine) and returns ChargeBreakdown | None.
"""

from backend.engine.handlers.unit_handlers import calc_per_unit, calc_per_unit_per_time, calc_per_unit_per_period
from backend.engine.handlers.regime_handlers import calc_multiple_regimes
from backend.engine.handlers.service_handlers import calc_per_service, calc_tiered_per_service
from backend.engine.handlers.cargo_handlers import (
    calc_per_commodity,
    calc_per_commodity_kl,
    calc_per_teu_flat,
    calc_per_leg,
)
from backend.engine.handlers.tiered_handlers import (
    calc_tiered,
    calc_tiered_per_100_tons_per_24h,
    calc_tiered_time,
)
from backend.engine.handlers.misc_handlers import calc_flat, calc_formula, calc_threshold_discount

__all__ = [
    "calc_per_unit",
    "calc_per_unit_per_time",
    "calc_per_unit_per_period",
    "calc_multiple_regimes",
    "calc_per_service",
    "calc_tiered_per_service",
    "calc_per_commodity",
    "calc_per_commodity_kl",
    "calc_per_teu_flat",
    "calc_per_leg",
    "calc_tiered",
    "calc_tiered_per_100_tons_per_24h",
    "calc_tiered_time",
    "calc_flat",
    "calc_formula",
    "calc_threshold_discount",
]
