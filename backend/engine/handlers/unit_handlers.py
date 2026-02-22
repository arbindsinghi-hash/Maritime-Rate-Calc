"""
Handlers for unit-based calculation types:
- per_unit (VTS Dues)
- per_unit_per_time (Port Dues)
- per_unit_per_period
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from backend.engine.handlers.common import build_citation
from backend.models.schemas import ChargeBreakdown

if TYPE_CHECKING:
    from backend.engine.tariff_engine import TariffEngine
    from backend.models.schemas import CalculationRequest
    from backend.models.tariff_rule import TariffSection


def calc_per_unit(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_unit: rate x basis (e.g., rate_per_gt x GT). Supports port_overrides and minimum_fee."""
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    port_id = req.operational_data.port_id.lower().replace(" ", "_")

    rate = calc.rate_per_gt or calc.rate or 0.0

    if port_id in calc.port_overrides:
        override = calc.port_overrides[port_id]
        if "rate_per_gt" in override:
            rate = override["rate_per_gt"]
        elif "rate" in override:
            rate = override["rate"]

    result = gt * rate

    if section.minimum_fee is not None and result < section.minimum_fee:
        result = section.minimum_fee

    result = round(result, 2)
    return ChargeBreakdown(
        charge=section.name,
        basis=gt,
        rate=rate,
        formula=f"GT({gt}) × {rate}/GT = {result}",
        result=result,
        citation=build_citation(section),
    )


def calc_per_unit_per_time(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_unit_per_time: base_rate x units + incremental_rate x units x time (Port Dues)."""
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    days = req.operational_data.days_alongside
    div = calc.divisor or 100
    base_r = calc.base_rate_per_100_tons or 0.0
    inc_r = calc.incremental_rate_per_100_tons_per_24h or 0.0
    units = gt / div

    base_amount = units * base_r
    inc_amount = units * inc_r * days
    result = base_amount + inc_amount

    result, red_descs = engine._apply_reductions(
        result, section.reductions, req, incremental_amount=inc_amount,
    )
    result, sur_descs = engine._apply_surcharges(
        result, section.surcharges, req,
        port_id=req.operational_data.port_id.lower().replace(" ", "_"),
        incremental_amount=inc_amount,
    )

    if section.minimum_fee is not None and result < section.minimum_fee:
        result = section.minimum_fee

    result = round(result, 2)

    formula = (
        f"(GT({gt})/{div}) × base({base_r}) + "
        f"(GT({gt})/{div}) × inc({inc_r}) × days({days})"
    )
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=gt,
        rate=base_r,
        rate_detail={"base_rate": base_r, "incremental_rate": inc_r, "days": days},
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_per_unit_per_period(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_unit_per_period: rate x basis x number_of_periods (e.g., per metre per year)."""
    calc = section.calculation
    rate = calc.rate or calc.rate_per_gt or 0.0
    loa = req.technical_specs.loa_meters
    gt = req.technical_specs.gross_tonnage

    if calc.basis == "loa_metres" or calc.basis == "length_overall_metres":
        basis_val = loa
        basis_label = f"LOA({loa}m)"
    else:
        basis_val = gt
        basis_label = f"GT({gt})"

    result = round(basis_val * rate, 2)
    return ChargeBreakdown(
        charge=section.name,
        basis=basis_val,
        rate=rate,
        formula=f"{basis_label} × {rate} = {result}",
        result=result,
        citation=build_citation(section),
    )
