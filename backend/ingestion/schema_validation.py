"""
schema_validation.py — Bridge module for ingestion pipeline.

Re-exports TariffSection and TariffRuleset from the canonical
tariff_rule.py models. The ingestion DAG validates LLM-extracted
YAML through these models before writing to disk.

Validates draft rules (charge_name, rate, basis, formula, citation);
rejects rules missing required fields (e.g. rate) via ValidationError.
"""

import logging
from typing import List, Tuple

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

from backend.models.tariff_rule import (
    TariffRuleset,
    TariffSection,
    TariffMetadata,
    TariffDefinitions,
    Calculation,
    CalculationType,
    Band,
    Regime,
    Reduction,
    Surcharge,
    Exemption,
    Citation,
)

# ── Draft rule schema (ingestion output) ─────────────────────────────────
# Used to validate clause_mapping output before promoting to TariffSection.


class TariffRuleDraftCitation(BaseModel):
    """Citation in draft rule."""
    page: int = 0
    section: str = ""


class TariffRule(BaseModel):
    """Draft tariff rule from clause mapping; rate is required (reject if missing)."""
    charge_name: str
    rate: float | str  # required; omit or None raises ValidationError
    basis: str = ""
    formula: str = ""
    citation: TariffRuleDraftCitation | dict = {}

    class Config:
        extra = "allow"

    @staticmethod
    def validate_rate_is_numeric(rate_value: float | str) -> bool:
        """Check if the rate can be interpreted as a valid positive number."""
        if isinstance(rate_value, (int, float)):
            return rate_value > 0
        if isinstance(rate_value, str):
            cleaned = rate_value.replace(",", "").replace(" ", "").strip()
            try:
                return float(cleaned) > 0
            except (ValueError, TypeError):
                return False
        return False


def _sanitize_section_dict(r: dict) -> dict:
    """Coerce LLM nulls to Pydantic-safe defaults before validation.

    Gemini often outputs ``special: null``, ``scope: null``, etc.
    Pydantic rejects None for str / Dict fields that have non-None defaults.
    """
    r = dict(r)  # shallow copy

    # Top-level str fields that should be "" not None
    for key in ("description", "note"):
        if key in r and r[key] is None:
            r[key] = ""

    # Top-level dict fields that should be {} not None
    if r.get("special") is None:
        r["special"] = {}

    # Nested: applicability
    app = r.get("applicability")
    if isinstance(app, dict):
        app = dict(app)
        for key in ("scope",):
            if key in app and app[key] is None:
                app[key] = ""
        for key in ("payable_by", "conditions"):
            if key in app and app[key] is None:
                app[key] = []
        r["applicability"] = app

    # Nested: calculation
    calc = r.get("calculation")
    if isinstance(calc, dict):
        calc = dict(calc)
        # type is required — if null, set to "unknown" so it doesn't crash
        if calc.get("type") is None:
            calc["type"] = "unknown"
        for key in ("basis",):
            if key in calc and calc[key] is None:
                calc[key] = ""
        r["calculation"] = calc
    elif calc is None:
        # No calculation block at all — add a placeholder
        r["calculation"] = {"type": "unknown"}

    # Nested: citation
    cite = r.get("citation")
    if isinstance(cite, dict):
        cite = dict(cite)
        if cite.get("page") is None:
            cite["page"] = 0
        if cite.get("section") is None:
            cite["section"] = ""
        r["citation"] = cite

    return r


def validate_draft_rules(draft_rules: List[dict]) -> Tuple[List[dict], List[dict]]:
    """
    Validate each draft rule/section.

    Accepts two formats:
      - New schema: dicts with id, name, calculation, etc. (TariffSection-compatible)
      - Legacy schema: dicts with charge_name, rate, basis, formula, citation

    Returns (validated_list, rejected_list).
    """
    validated: List[dict] = []
    rejected: List[dict] = []
    for r in draft_rules:
        # ── New schema: has 'id' or 'calculation' key ──
        if r.get("id") or r.get("calculation"):
            sanitized = _sanitize_section_dict(r)
            try:
                section = TariffSection(**sanitized)
                validated.append(sanitized)
            except (ValidationError, TypeError) as exc:
                logger.warning("Rejected section '%s': %s", r.get("id") or r.get("name", ""), exc)
                rejected.append(r)
            continue

        # ── Legacy schema: charge_name + rate ──
        try:
            cite = r.get("citation") or {}
            if isinstance(cite, dict):
                cite = {**cite, "page": cite.get("page", 0), "section": cite.get("section", "")}
            rate_value = r["rate"]
            rule = TariffRule(
                charge_name=r.get("charge_name", ""),
                rate=rate_value,
                basis=r.get("basis", ""),
                formula=r.get("formula", ""),
                citation=cite,
            )
            if not TariffRule.validate_rate_is_numeric(rate_value):
                logger.warning(
                    "Rejected rule '%s': rate '%s' is not a valid positive number",
                    r.get("charge_name", ""), rate_value,
                )
                rejected.append(r)
                continue
            validated.append(r)
        except (ValidationError, TypeError, KeyError) as exc:
            logger.warning("Rejected rule '%s': %s", r.get("charge_name", ""), exc)
            rejected.append(r)
    return validated, rejected


__all__ = [
    "TariffRuleset",
    "TariffSection",
    "TariffRule",
    "TariffRuleDraftCitation",
    "TariffMetadata",
    "TariffDefinitions",
    "Calculation",
    "CalculationType",
    "Band",
    "Regime",
    "Reduction",
    "Surcharge",
    "Exemption",
    "Citation",
    "validate_draft_rules",
    "ValidationError",
]
