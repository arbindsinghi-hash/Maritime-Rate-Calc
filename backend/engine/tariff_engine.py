"""
Deterministic Tariff Engine
===========================
Loads the Golden YAML (TariffRuleset) and computes charges deterministically.

Architecture:
  1. Rule Loader — TariffRuleset.from_yaml() → typed Pydantic objects
  2. Generic Dispatch — dispatch table maps CalculationType → handler
  3. Handlers — modular handlers in engine/handlers/ (per_unit, per_service, etc.)
  4. Framework — reductions, surcharges, exemptions with stacking rules
  5. Condition Evaluator — data-driven condition matching in condition_evaluator.py
  6. Orchestrator — calculate() iterates applicable sections, dispatches, applies framework

Target: SUDESTADA vessel total ≈ 506,830.83 ZAR (±1%)
"""
from __future__ import annotations

import os
import logging
from typing import Callable, Dict, List, Optional, Tuple

from backend.core.config import settings
from backend.engine.condition_evaluator import condition_matches, all_conditions_match
from backend.engine.handlers import (
    calc_per_unit,
    calc_per_unit_per_time,
    calc_per_unit_per_period,
    calc_multiple_regimes,
    calc_per_service,
    calc_tiered_per_service,
    calc_per_commodity,
    calc_per_commodity_kl,
    calc_per_teu_flat,
    calc_per_leg,
    calc_tiered,
    calc_tiered_per_100_tons_per_24h,
    calc_tiered_time,
    calc_flat,
    calc_formula,
    calc_threshold_discount,
)
from backend.models.schemas import CalculationRequest, ChargeBreakdown
from backend.models.tariff_rule import (
    TariffRuleset,
    TariffSection,
    Reduction,
    Surcharge,
    CalculationType,
)

logger = logging.getLogger(__name__)


# Sections the engine computes for a standard port call.
# Specialty charges (cargo dues, drydock, fire services, etc.) are only
# computed when explicitly applicable.
STANDARD_PORT_CALL_SECTIONS = {
    "light_dues",
    "vts_charges",
    "pilotage",
    "tugs_assistance",
    "berthing_services",
    "port_dues",
}


class TariffEngine:
    """
    Deterministic tariff computation engine.

    Loads a TariffRuleset from YAML and dispatches to typed handlers
    based on each section's calculation.type.
    """

    def __init__(self, version: str = "latest"):
        self.version = version
        self.ruleset: Optional[TariffRuleset] = None
        self._load_rules()

        # Dispatch table: CalculationType value → handler function
        self._dispatch: Dict[str, Callable] = {
            CalculationType.PER_UNIT.value: calc_per_unit,
            CalculationType.PER_UNIT_PER_TIME.value: calc_per_unit_per_time,
            CalculationType.PER_UNIT_PER_PERIOD.value: calc_per_unit_per_period,
            CalculationType.MULTIPLE_REGIMES.value: calc_multiple_regimes,
            CalculationType.PER_SERVICE.value: calc_per_service,
            CalculationType.TIERED_PER_SERVICE.value: calc_tiered_per_service,
            CalculationType.TIERED.value: calc_tiered,
            CalculationType.TIERED_PER_100_TONS_PER_24H.value: calc_tiered_per_100_tons_per_24h,
            CalculationType.TIERED_TIME.value: calc_tiered_time,
            CalculationType.PER_COMMODITY.value: calc_per_commodity,
            CalculationType.PER_COMMODITY_PER_KL.value: calc_per_commodity_kl,
            CalculationType.PER_TEU_FLAT.value: calc_per_teu_flat,
            CalculationType.PER_LEG.value: calc_per_leg,
            CalculationType.THRESHOLD_DISCOUNT.value: calc_threshold_discount,
            CalculationType.FLAT.value: calc_flat,
            CalculationType.FORMULA.value: calc_formula,
        }

    # ── Rule Loader ──────────────────────────────────────────────────────

    def _load_rules(self) -> None:
        """Load YAML via TariffRuleset.from_yaml for typed, validated access."""
        yaml_path = os.path.join(settings.YAML_DIR, f"tariff_rules_{self.version}.yaml")
        if not os.path.exists(yaml_path):
            logger.error(
                "╔══════════════════════════════════════════════════════════╗\n"
                "║  TARIFF YAML NOT FOUND — engine will NOT serve requests  ║\n"
                "║  Expected: %-44s ║\n"
                "╚══════════════════════════════════════════════════════════╝",
                yaml_path,
            )
            self.ruleset = None
            return
        try:
            self.ruleset = TariffRuleset.from_yaml(yaml_path)
            logger.info(
                "Loaded tariff ruleset v%s: %d sections",
                self.version, len(self.ruleset.sections),
            )
        except Exception as e:
            logger.error("Failed to load tariff YAML: %s", e)
            self.ruleset = None

    # ── Exemptions ───────────────────────────────────────────────────────

    def _check_exemptions(self, section: TariffSection, req: CalculationRequest) -> bool:
        """Return True if vessel is exempt from this charge (charge = 0)."""
        for exemption in section.exemptions:
            if exemption.conditions and all_conditions_match(exemption.conditions, req):
                return True
        return False

    # ── Reductions with Stacking Rules ───────────────────────────────────

    def _apply_reductions(
        self,
        base_amount: float,
        reductions: List[Reduction],
        req: CalculationRequest,
        incremental_amount: Optional[float] = None,
    ) -> Tuple[float, List[str]]:
        """
        Apply reductions with stacking rules.

        Rules:
        - 100% reductions are exemptions (return 0)
        - Non-stackable reductions: take the BEST single one
        - Stackable reductions: accumulate additively
        - not_stackable_with: if a non-stackable is chosen, skip conflicting ones
        - max_total_pct: cap the total stackable reduction
        - applies_to == "incremental_fee_only": only reduce the incremental part
        """
        if not reductions:
            return base_amount, []

        applicable: List[Reduction] = []
        for r in reductions:
            if r.conditions and all_conditions_match(r.conditions, req):
                applicable.append(r)

        if not applicable:
            return base_amount, []

        for r in applicable:
            if r.percentage >= 100:
                return 0.0, [r.description or f"100% reduction: {r.id}"]

        stackable = [r for r in applicable if r.stackable]
        non_stackable = [r for r in applicable if not r.stackable]

        best_ns: Optional[Reduction] = None
        if non_stackable:
            best_ns = max(non_stackable, key=lambda r: r.percentage)

        blocked_ids = set()
        if best_ns and best_ns.not_stackable_with:
            blocked_ids = set(best_ns.not_stackable_with)

        valid_stackable = [s for s in stackable if s.id not in blocked_ids]

        total_pct = 0.0
        applied_descs: List[str] = []

        if best_ns:
            total_pct += best_ns.percentage
            applied_descs.append(best_ns.description or f"{best_ns.percentage}% ({best_ns.id})")

        for s in valid_stackable:
            add_pct = s.percentage
            if s.max_total_pct is not None:
                add_pct = min(add_pct, s.max_total_pct)
            total_pct += add_pct
            applied_descs.append(s.description or f"{add_pct}% ({s.id})")

        if incremental_amount is not None:
            has_inc_only = any(r.applies_to == "incremental_fee_only" for r in applicable)
            if has_inc_only:
                reduced_inc = incremental_amount * (1 - total_pct / 100)
                return base_amount - incremental_amount + reduced_inc, applied_descs

        reduced = base_amount * (1 - total_pct / 100)
        return round(reduced, 2), applied_descs

    # ── Surcharges ───────────────────────────────────────────────────────

    def _apply_surcharges(
        self,
        amount: float,
        surcharges: List[Surcharge],
        req: CalculationRequest,
        port_id: str = "",
        incremental_amount: Optional[float] = None,
    ) -> Tuple[float, List[str]]:
        """Apply applicable surcharges. Returns (surcharged_amount, descriptions)."""
        if not surcharges:
            return amount, []

        total_surcharge = 0.0
        descs: List[str] = []

        for s in surcharges:
            if s.port_id and s.port_id.lower() != port_id:
                continue
            if s.conditions and all_conditions_match(s.conditions, req):
                if s.applies_to == "incremental_fee_only" and incremental_amount is not None:
                    sc = incremental_amount * (s.percentage / 100)
                else:
                    sc = amount * (s.percentage / 100)
                total_surcharge += sc
                descs.append(f"+{s.percentage}%: {', '.join(s.conditions)}")

        return round(amount + total_surcharge, 2), descs

    # ── VAT ──────────────────────────────────────────────────────────────

    def apply_vat(self, amount: float, rate: Optional[float] = None) -> float:
        """Apply VAT to an amount."""
        if rate is None and self.ruleset and self.ruleset.metadata.vat_pct:
            rate = self.ruleset.metadata.vat_pct / 100
        elif rate is None:
            rate = 0.15
        return round(amount * (1 + rate), 2)

    # ── Config derived from YAML ────────────────────────────────────────

    def get_form_config(self) -> dict:
        """
        Return ports, vessel_types, and purposes derived from the loaded
        TariffRuleset to prevent hardcoded lists.
        """
        from backend.models.schemas import VesselType, VisitPurpose

        ports: dict[str, str] = {}
        if self.ruleset:
            # Collect port IDs from port_rates, port_overrides, base_fee_by_port,
            # rate_per_100_tons_above, and working_hours across all sections.
            for section in self.ruleset.sections:
                calc = section.calculation
                for pid in calc.port_rates:
                    ports.setdefault(pid, pid.replace("_", " ").title())
                for pid in calc.port_overrides:
                    ports.setdefault(pid, pid.replace("_", " ").title())
                for band in calc.bands:
                    if band.base_fee_by_port:
                        for pid in band.base_fee_by_port:
                            ports.setdefault(pid, pid.replace("_", " ").title())
                    if band.rate_per_100_tons_above:
                        for pid in band.rate_per_100_tons_above:
                            ports.setdefault(pid, pid.replace("_", " ").title())

            # Also pull from definitions.working_hours (excludes "default")
            for key in self.ruleset.definitions.working_hours:
                if key != "default":
                    ports.setdefault(key, key.replace("_", " ").title())

        # Ensure "other" is always present and last
        ports.pop("other", None)
        sorted_ports = dict(sorted(ports.items(), key=lambda kv: kv[1]))
        sorted_ports["other"] = "Other"

        # Vessel types: merge YAML definitions with VesselType enum so
        # types like "container" (not in YAML defs but valid) are included.
        seen_ids: set[str] = set()
        vessel_types: list[dict[str, str]] = []
        if self.ruleset:
            for vt in self.ruleset.definitions.vessel_types:
                label = vt.id.replace("_", " ").title()
                vessel_types.append({"id": vt.id, "label": label})
                seen_ids.add(vt.id)
        # Add any enum members not already covered by YAML definitions
        for member in VesselType:
            if member.value not in seen_ids:
                vessel_types.append({
                    "id": member.value,
                    "label": member.value.replace("_", " ").title(),
                })

        # Purposes from the VisitPurpose enum (operational, not in YAML)
        purposes = [
            {"id": member.value, "label": member.value.replace("_", " ").title()}
            for member in VisitPurpose
        ]

        return {
            "ports": sorted_ports,
            "vessel_types": vessel_types,
            "purposes": purposes,
        }

    # ── Orchestrator ─────────────────────────────────────────────────────

    def calculate(self, request: CalculationRequest) -> List[ChargeBreakdown]:
        """
        Main entry point: iterate all standard port-call sections,
        check exemptions, dispatch to handler, collect breakdowns.
        """
        if not self.ruleset:
            logger.error("No ruleset loaded — cannot calculate")
            return []

        breakdown: List[ChargeBreakdown] = []

        for section in self.ruleset.sections:
            if section.id not in STANDARD_PORT_CALL_SECTIONS:
                continue

            if self._check_exemptions(section, request):
                logger.debug("Section %s: EXEMPT", section.id)
                continue

            ctype = section.calculation.type
            handler = self._dispatch.get(ctype)
            if handler is None:
                logger.warning(
                    "No handler for calculation type '%s' (section %s)",
                    ctype, section.id,
                )
                continue

            result = handler(section, request, self)
            if result is not None and result.result > 0:
                if section.minimum_fee is not None and result.result < section.minimum_fee:
                    result.result = round(section.minimum_fee, 2)
                    result.formula += f" [min_fee applied: {section.minimum_fee}]"
                if section.maximum_fee is not None and result.result > section.maximum_fee:
                    result.result = round(section.maximum_fee, 2)
                    result.formula += f" [max_fee applied: {section.maximum_fee}]"
                breakdown.append(result)

        return breakdown


# Module-level singleton
tariff_engine = TariffEngine()
