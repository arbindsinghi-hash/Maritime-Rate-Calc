"""
Handlers for tiered calculation types:
- tiered (Late order penalties — percentage tiers by days)
- tiered_per_100_tons_per_24h (Berth dues — tiered GT per 24h)
- tiered_time (Drydock, slipway, visiting vessel time-based tiers)
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


def calc_tiered(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """tiered: percentage-based tiers (e.g., late order penalties by days late).

    YAML bands have max_value (days) and rate_per_unit_above (percentage).
    The penalty is a percentage of the original cargo dues order.
    """
    calc = section.calculation
    bands = calc.bands
    if not bands:
        return None

    basis_type = calc.basis or "days_late"
    if basis_type == "days_late":
        basis_val = req.operational_data.days_alongside
    else:
        basis_val = req.technical_specs.gross_tonnage

    prev_max = 0.0
    rate = 0.0
    matched_band = None

    for band in bands:
        max_val = band.upper_bound
        if max_val is None or basis_val <= max_val:
            rate = band.rate_per_unit_above or 0.0
            matched_band = band
            break
        prev_max = max_val

    if rate <= 0:
        return None

    result = round(basis_val * rate, 2)

    return ChargeBreakdown(
        charge=section.name,
        basis=basis_val,
        rate=rate,
        rate_detail={"basis_type": basis_type, "tiered": True},
        formula=f"{basis_val} {basis_type} × R{rate} = {result}",
        result=result,
        citation=build_citation(section),
    )


def calc_tiered_per_100_tons_per_24h(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """tiered_per_100_tons_per_24h: berth dues — find GT band, compute rate x GT/100 x 24h periods.

    YAML structure:
        bands:
          - max_tonnage: 17700
            base_fee: 1234.56
            rate_per_100_tons: 7.89
        divisor: 100
        period: 24_hours
    """
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    days = req.operational_data.days_alongside
    div = calc.divisor or 100
    bands = calc.bands

    if not bands:
        return None

    prev_max = 0.0
    base_fee = 0.0
    rate_per_100 = 0.0

    for band in bands:
        max_t = band.upper_bound
        if max_t is None or gt <= max_t:
            base_fee = band.base_fee or 0.0
            rate_per_100 = band.rate_per_100_tons or 0.0
            break
        prev_max = max_t

    units = gt / div
    result = (base_fee + units * rate_per_100) * days

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"(base({base_fee}) + GT({gt})/{div} × R{rate_per_100}) × {days} days"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=gt,
        rate=rate_per_100,
        rate_detail={
            "base_fee": base_fee,
            "rate_per_100_tons": rate_per_100,
            "days": days,
            "period": "24_hours",
        },
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_tiered_time(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """tiered_time: time-based tiers for drydock/slipway/visiting vessels.

    YAML structure:
        basis: gross_tonnage_cubic_metres
        per: time_period
        bands:
          - max_tonnage: 3000
            base_fee: 8585.20
            rate_per_unit_above: 2.86
    """
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    days = req.operational_data.days_alongside
    bands = calc.bands

    if not bands:
        return None

    prev_max = 0.0
    base_fee = 0.0
    rate_above = 0.0
    gt_above_floor = 0.0

    for band in bands:
        max_t = band.upper_bound
        if max_t is None or gt <= max_t:
            base_fee = band.base_fee or 0.0
            rate_above = band.rate_per_unit_above or 0.0
            gt_above_floor = max(0.0, gt - prev_max)
            break
        prev_max = max_t

    per_period = base_fee + gt_above_floor * rate_above
    result = round(per_period * days, 2)

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"(base({base_fee}) + {gt_above_floor} × R{rate_above}) × {days} days"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=gt,
        rate=rate_above,
        rate_detail={
            "base_fee": base_fee,
            "rate_per_unit_above": rate_above,
            "gt_above_floor": gt_above_floor,
            "days": days,
        },
        formula=formula,
        result=result,
        citation=build_citation(section),
    )
