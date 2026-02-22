"""
Handlers for service-based calculation types:
- per_service (Pilotage, Berthing, Running Lines)
- tiered_per_service (Tugs)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from backend.engine.handlers.common import build_citation
from backend.models.schemas import ChargeBreakdown
from backend.models.tariff_rule import PortRate

if TYPE_CHECKING:
    from backend.engine.tariff_engine import TariffEngine
    from backend.models.schemas import CalculationRequest
    from backend.models.tariff_rule import TariffSection


def calc_per_service(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_service: (base_fee + rate_per_100_tons x GT/divisor) x num_services."""
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    port_id = req.operational_data.port_id.lower().replace(" ", "_")
    num_ops = req.operational_data.num_operations or 2
    sid = section.id

    pr: Optional[PortRate] = None
    if port_id in calc.port_rates:
        pr = calc.port_rates[port_id]
    elif "other" in calc.port_rates:
        pr = calc.port_rates["other"]

    if pr is None:
        return None

    base_fee = pr.base_fee or 0.0
    rp100 = pr.rate_per_100_tons or 0.0
    div = calc.divisor or 100

    if sid == "running_lines":
        n_services = num_ops
        per_service = base_fee
        result = per_service * n_services
        formula = f"{num_ops} ops × base({base_fee})"
        rate_val = base_fee
        rate_detail = {
            "base_fee": base_fee,
            "num_operations": num_ops,
            "total_services": n_services,
        }
    else:
        per_service = base_fee + (gt / div) * rp100
        n_services = num_ops
        result = per_service * n_services
        formula = f"{n_services} × (base({base_fee}) + GT({gt})/{div} × {rp100})"
        rate_val = rp100
        rate_detail = {
            "base_fee": base_fee,
            "rate_per_100_tons": rp100,
            "per_service": round(per_service, 2),
            "num_services": n_services,
            "port_id": port_id,
        }

    result, sur_descs = engine._apply_surcharges(
        result, section.surcharges, req, port_id=port_id,
    )

    result = round(result, 2)
    if sur_descs:
        formula += f" | Surcharges: {'; '.join(sur_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=gt if sid != "running_lines" else float(n_services),
        rate=rate_val,
        rate_detail=rate_detail,
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_tiered_per_service(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """tiered_per_service: tonnage band → base_fee + rate_above x (GT - floor) / div x ops."""
    calc = section.calculation
    gt = req.technical_specs.gross_tonnage
    port_id = req.operational_data.port_id.lower().replace(" ", "_")
    num_tug_ops = req.operational_data.num_tug_operations or req.operational_data.num_operations or 2
    div = calc.divisor or 100

    craft_units = 1.0
    for ca in calc.craft_allocation:
        max_t = ca.max_tonnage
        if max_t is None or gt <= max_t:
            craft_units = ca.craft_units
            break

    bands = calc.bands
    prev_max = 0.0
    base_fee = 0.0
    rate_above = 0.0
    gt_above_floor = 0.0
    matched_band_idx = -1

    for i, band in enumerate(bands):
        max_t = band.upper_bound
        if max_t is None or gt <= max_t:
            bp = band.base_fee_by_port or {}
            base_fee = bp.get(port_id) or bp.get("other") or band.base_fee or 0.0

            rpa = band.rate_per_100_tons_above or {}
            if isinstance(rpa, dict):
                rate_above = rpa.get(port_id) or rpa.get("other") or 0.0
            else:
                rate_above = rpa or 0.0

            gt_above_floor = max(0.0, gt - prev_max)
            matched_band_idx = i
            break
        prev_max = max_t

    per_service = base_fee + (gt_above_floor / div) * rate_above
    result = per_service * num_tug_ops

    result, sur_descs = engine._apply_surcharges(
        result, section.surcharges, req, port_id=port_id,
    )

    result = round(result, 2)

    formula = (
        f"(base({base_fee}) + ({gt_above_floor}/{div}) × {rate_above}) "
        f"× {num_tug_ops} ops [craft_allocation: {craft_units}]"
    )
    if sur_descs:
        formula += f" | Surcharges: {'; '.join(sur_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=gt,
        rate=rate_above,
        rate_detail={
            "base_fee": base_fee,
            "rate_per_100_tons_above": rate_above,
            "gt_above_floor": gt_above_floor,
            "craft_units": craft_units,
            "num_operations": num_tug_ops,
            "per_service": round(per_service, 2),
            "band_index": matched_band_idx,
            "port_id": port_id,
        },
        formula=formula,
        result=result,
        citation=build_citation(section),
    )
