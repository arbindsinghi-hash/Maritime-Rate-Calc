"""
Handlers for cargo-based calculation types:
- per_commodity_per_ton (Breakbulk, Dry Bulk cargo dues)
- per_commodity_per_kilolitre (Liquid Bulk cargo dues)
- per_teu_flat (Container dues)
- per_leg (Coastwise/Transhipment dues)
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


def _resolve_commodity_rate(
    calc_data: dict,
    commodity: str,
    direction: str,
) -> float:
    """Look up the rate for a specific commodity and direction (import/export).

    Falls back to base_rates if no commodity-specific rate found.
    """
    base_rates = calc_data.get("base_rates", {})
    commodities = calc_data.get("commodities", [])

    for comm in commodities:
        if not isinstance(comm, dict):
            continue
        comm_name = comm.get("name", "").lower()
        if commodity.lower() in comm_name or comm_name in commodity.lower():
            rate_key = f"{direction}_rate"
            if rate_key in comm:
                return float(comm[rate_key])
            if "rate" in comm:
                return float(comm["rate"])

    return float(base_rates.get(f"{direction}s", base_rates.get(direction, 0.0)))


def calc_per_commodity(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_commodity_per_ton: rate_per_ton x cargo_quantity for breakbulk/dry bulk."""
    calc = section.calculation
    cargo_qty = req.operational_data.cargo_quantity_mt
    if not cargo_qty or cargo_qty <= 0:
        return None

    commodity = req.operational_data.commodity or req.operational_data.cargo_type or ""
    activity = req.operational_data.activity.lower()
    direction = "export" if "export" in activity or "load" in activity else "import"

    rate = _resolve_commodity_rate(calc.model_dump(), commodity, direction)
    if rate <= 0:
        return None

    result = round(cargo_qty * rate, 2)

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"{cargo_qty} MT × R{rate}/MT ({direction})"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=cargo_qty,
        rate=rate,
        rate_detail={"commodity": commodity, "direction": direction, "unit": "metric_ton"},
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_per_commodity_kl(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_commodity_per_kilolitre: rate_per_kl x cargo_quantity for liquid bulk."""
    calc = section.calculation
    cargo_qty = req.operational_data.cargo_quantity_mt
    if not cargo_qty or cargo_qty <= 0:
        return None

    commodity = req.operational_data.commodity or req.operational_data.cargo_type or ""
    activity = req.operational_data.activity.lower()
    direction = "export" if "export" in activity or "load" in activity else "import"

    rate = _resolve_commodity_rate(calc.model_dump(), commodity, direction)
    if rate <= 0:
        return None

    result = round(cargo_qty * rate, 2)

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"{cargo_qty} kL × R{rate}/kL ({direction})"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=cargo_qty,
        rate=rate,
        rate_detail={"commodity": commodity, "direction": direction, "unit": "kilolitre"},
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_per_teu_flat(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_teu_flat: flat rate per TEU/container size and direction.

    YAML rates keys example:
        6m_20ft_import: 1817.91
        6m_20ft_export: 399.79
        12m_40ft_import: 3635.80
        12m_40ft_export: 799.57
    """
    calc = section.calculation
    cargo_qty = req.operational_data.cargo_quantity_mt
    if not cargo_qty or cargo_qty <= 0:
        return None

    activity = req.operational_data.activity.lower()
    direction = "export" if "export" in activity or "load" in activity else "import"

    rates = calc.rates
    rate_key_20 = f"6m_20ft_{direction}"
    rate_key_40 = f"12m_40ft_{direction}"

    rate_20 = rates.get(rate_key_20, 0.0)
    rate_40 = rates.get(rate_key_40, 0.0)

    rate = rate_20 if rate_20 > 0 else rate_40
    if rate <= 0:
        return None

    teu_count = cargo_qty
    result = round(teu_count * rate, 2)

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"{teu_count} TEU × R{rate} ({direction})"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=teu_count,
        rate=rate,
        rate_detail={
            "direction": direction,
            "rate_20ft": rate_20,
            "rate_40ft": rate_40,
        },
        formula=formula,
        result=result,
        citation=build_citation(section),
    )


def calc_per_leg(
    section: TariffSection,
    req: CalculationRequest,
    engine: TariffEngine,
) -> Optional[ChargeBreakdown]:
    """per_leg: flat rate per leg for coastwise/transhipment cargo.

    YAML rates keys example:
        breakbulk_bulk_per_ton_per_leg: 16.79
        container_6m_20ft_per_leg: 74.64
        container_12m_40ft_per_leg: 149.26
        other_cargo_per_ton_per_leg: 4.31
    """
    calc = section.calculation
    cargo_qty = req.operational_data.cargo_quantity_mt
    if not cargo_qty or cargo_qty <= 0:
        return None

    cargo_type = (req.operational_data.cargo_type or "other").lower()
    rates = calc.rates

    if "container" in cargo_type:
        rate = rates.get("container_6m_20ft_per_leg", 0.0)
    elif "bulk" in cargo_type or "breakbulk" in cargo_type:
        rate = rates.get("breakbulk_bulk_per_ton_per_leg", 0.0)
    else:
        rate = rates.get("other_cargo_per_ton_per_leg", 0.0)

    if rate <= 0:
        return None

    result = round(cargo_qty * rate, 2)

    result, red_descs = engine._apply_reductions(result, section.reductions, req)
    result = round(result, 2)

    formula = f"{cargo_qty} × R{rate}/leg ({cargo_type})"
    if red_descs:
        formula += f" | Reductions: {'; '.join(red_descs)}"

    return ChargeBreakdown(
        charge=section.name,
        basis=cargo_qty,
        rate=rate,
        rate_detail={"cargo_type": cargo_type, "per": "per_leg"},
        formula=formula,
        result=result,
        citation=build_citation(section),
    )
