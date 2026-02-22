"""
Handler for multiple_regimes calculation type (Light Dues).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from backend.engine.handlers.common import build_citation
from backend.models.schemas import ChargeBreakdown
from backend.models.tariff_rule import Regime

if TYPE_CHECKING:
    from backend.engine.tariff_engine import TariffEngine
    from backend.models.schemas import CalculationRequest
    from backend.models.tariff_rule import TariffSection


def calc_multiple_regimes(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """multiple_regimes: evaluate the matching regime (Light Dues)."""
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    loa = req.technical_specs.loa_meters

    chosen_regime: Optional[Regime] = None
    for regime in calc.regimes:
        if regime.id == "registered_port":
            continue
        if regime.id in ("all_other_vessels", "first_sa_port_to_last"):
            chosen_regime = regime
            break

    if chosen_regime is None and calc.regimes:
        chosen_regime = calc.regimes[-1]

    if chosen_regime is None:
        return None

    if chosen_regime.basis == "loa_metres":
        rate = chosen_regime.rate_per_metre or 0.0
        result = loa * rate
        formula = f"LOA({loa}m) × {rate}/m"
        basis_val = loa
        rate_val = rate
    else:
        div = chosen_regime.divisor or 100
        rate = chosen_regime.rate_per_100_tons or 0.0
        result = (gt / div) * rate
        formula = f"GT({gt}) / {div} × {rate}/100GT"
        basis_val = gt
        rate_val = rate

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=basis_val,
        rate=rate_val,
        rate_detail={"regime": chosen_regime.id, "validity": chosen_regime.validity},
        formula=formula,
        result=result,
        citation=build_citation(section),
    )
