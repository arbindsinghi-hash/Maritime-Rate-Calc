"""
Handlers for miscellaneous calculation types:
- flat (License fees, fire services, security, small vessel rates)
- formula (Custom formula-based — not used in current YAML)
- threshold_discount (Marine Services Incentive — shipping line level)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from backend.engine.handlers.common import build_citation
from backend.models.schemas import ChargeBreakdown

if TYPE_CHECKING:
    from backend.engine.tariff_engine import TariffEngine
    from backend.models.schemas import CalculationRequest
    from backend.models.tariff_rule import TariffSection

logger = logging.getLogger(__name__)


def calc_flat(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """flat: fixed fee from section.calculation.rate or rates dict.

    YAML variants:
        - Single rate:  rate: 22576.31, per: "per_licence_per_year"
        - Rates dict:   rates: { fire_tender_heavy_duty_turnout: 3762.19, ... }
    """
    calc = section.calculation

    if calc.rate and calc.rate > 0:
        result = round(calc.rate, 2)
        return ChargeBreakdown(
            charge=section.name,
            basis=1.0,
            rate=calc.rate,
            rate_detail={"per": calc.per or "flat"},
            formula=f"Flat fee: R{calc.rate} ({calc.per or 'per unit'})",
            result=result,
            citation=build_citation(section),
        )

    rates = calc.rates
    if rates:
        first_key = next(iter(rates))
        first_rate = rates[first_key]
        result = round(first_rate, 2)
        return ChargeBreakdown(
            charge=section.name,
            basis=1.0,
            rate=first_rate,
            rate_detail={"rates": rates, "selected": first_key},
            formula=f"Flat fee: R{first_rate} ({first_key})",
            result=result,
            citation=build_citation(section),
        )

    return None


def calc_formula(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """formula: custom formula-based calculation.

    Not used in the current YAML. Returns None with a log warning if encountered.
    """
    logger.warning(
        "Formula-based calculation not implemented for section '%s' — "
        "this type requires custom formula evaluation",
        section.id,
    )
    return None


def calc_threshold_discount(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """threshold_discount: Marine Services Incentive — applied at shipping line level.

    This is a discount scheme based on cumulative calls by a shipping line.
    It applies as a percentage reduction on pilotage, tugs, and berthing.
    Not computed per-vessel — requires shipping line annual statistics.
    Returns None as this is not a per-call charge.
    """
    return None
