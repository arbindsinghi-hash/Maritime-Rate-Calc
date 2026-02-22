"""
section_template.py — Minimum-required JSON templates for LLM-driven tariff extraction.

STATUS: LEGACY — This is an alternative ingestion path that uses per-section
JSON templates filled by an LLM. The canonical ingestion pipeline uses the
LangGraph DAG defined in dag.py (gemini_extract → page_fusion → clause_mapping).

This module is retained for reference and may be useful for future section-level
extraction improvements. It is NOT wired into the production DAG.

Architecture:
    PDF section text  →  LLM fills JSON template  →  Pydantic validates  →  YAML output

The LLM is given:
  1. The extracted markdown text for ONE section of the tariff book
  2. A JSON template with field descriptions and allowed values
  3. Instructions to fill ONLY from the provided text (no hallucination)

The template is the MINIMUM structure needed. The LLM's job is to populate
values — not invent structure. This constrains the output space dramatically,
making even probabilistic LLMs reliable for deterministic rule extraction.

Design Principles:
  - ONE template per section (not one per calculation type)
  - Every field has a type hint + description + allowed values where applicable
  - The template mirrors TariffSection from tariff_rule.py exactly
  - Optional fields are explicitly marked — LLM returns null if not found in text
  - Numeric fields are always float (never string) to catch extraction errors early
"""

from __future__ import annotations

import json
from typing import Any, Dict

# ─────────────────────────────────────────────────────────────────────────────
# The 16 valid calculation types (mirrors CalculationType enum)
# ─────────────────────────────────────────────────────────────────────────────
CALCULATION_TYPES = [
    "per_unit",
    "per_unit_per_time",
    "per_unit_per_period",
    "tiered",
    "tiered_per_service",
    "tiered_per_100_tons_per_24h",
    "tiered_time",
    "per_service",
    "per_commodity_per_ton",
    "per_commodity_per_kilolitre",
    "per_teu_flat",
    "per_leg",
    "threshold_discount",
    "multiple_regimes",
    "flat",
    "formula",
]


# ─────────────────────────────────────────────────────────────────────────────
# The minimum-required JSON template for a single TariffSection
# ─────────────────────────────────────────────────────────────────────────────

SECTION_TEMPLATE: Dict[str, Any] = {
    "_instructions": (
        "Fill every field from the provided tariff section text. "
        "Use null for fields not mentioned in the text. "
        "All monetary values must be numbers (float), never strings. "
        "All percentages are numbers (e.g., 35 not '35%'). "
        "Condition strings should be lowercase_snake_case descriptors."
    ),

    # ── Identity ─────────────────────────────────────────────────────────
    "id": {
        "_type": "string",
        "_description": "Machine-readable identifier, lowercase_snake_case",
        "_example": "light_dues",
        "value": None,
    },
    "name": {
        "_type": "string",
        "_description": "Human-readable charge name as it appears in the tariff book",
        "_example": "Light Dues",
        "value": None,
    },
    "description": {
        "_type": "string",
        "_description": "1-3 sentence summary of what this charge covers",
        "value": None,
    },

    # ── Citation ─────────────────────────────────────────────────────────
    "citation": {
        "_description": "Where in the tariff book this section is defined",
        "page": {"_type": "integer", "value": None},
        "section": {"_type": "string", "_example": "3.3", "value": None},
    },

    # ── Applicability ────────────────────────────────────────────────────
    "applicability": {
        "_description": "Who pays and under what conditions",
        "payable_by": {
            "_type": "list[string]",
            "_allowed": [
                "vessel_owner", "vessel_agent", "shipping_line",
                "importer", "exporter", "clearing_agent",
                "licence_holder", "container_operator",
            ],
            "value": [],
        },
        "conditions": {
            "_type": "list[string]",
            "_description": "Pre-conditions for this charge to apply (snake_case)",
            "value": [],
        },
        "scope": {
            "_type": "string",
            "_description": "Which ports: 'all_sa_ports' or comma-separated port ids",
            "_example": "all_sa_ports",
            "value": None,
        },
        "cargo_working_vessels_only": {
            "_type": "boolean",
            "_default": False,
            "value": False,
        },
    },

    # ── Calculation ──────────────────────────────────────────────────────
    "calculation": {
        "_description": "The core calculation specification. Type determines which fields are required.",
        "type": {
            "_type": "string",
            "_allowed": CALCULATION_TYPES,
            "_description": "Calculation strategy — determines which sub-fields below are required",
            "value": None,
        },

        # Common fields (used by multiple types)
        "basis": {
            "_type": "string|null",
            "_description": "What the rate is applied to: 'gross_tonnage', 'length_overall_metres', 'service', 'days_late'",
            "value": None,
        },
        "divisor": {
            "_type": "float|null",
            "_description": "Divide basis by this before applying rate (e.g., 100 for 'per 100 GT')",
            "value": None,
        },
        "per": {
            "_type": "string|null",
            "_description": "Per what: 'port_call', 'service', 'per_metre_per_day', 'per_hour', 'time_period'",
            "value": None,
        },

        # ── For per_unit ─────────────────────────────────────────────────
        "rate": {"_type": "float|null", "value": None},
        "rate_per_gt": {"_type": "float|null", "value": None},
        "rate_per_100_tons": {"_type": "float|null", "value": None},

        # ── For per_unit_per_time ────────────────────────────────────────
        "base_rate_per_100_tons": {"_type": "float|null", "value": None},
        "incremental_rate_per_100_tons_per_24h": {"_type": "float|null", "value": None},
        "period": {"_type": "string|null", "_example": "24_hours", "value": None},
        "period_rounding": {"_type": "string|null", "_example": "pro_rata", "value": None},

        # ── For tiered / tiered_per_service / tiered_time / tiered_per_100_tons_per_24h
        "bands": {
            "_type": "list[object]|null",
            "_description": "Ordered list of tiers/bands. max_tonnage/max_value=null means unbounded.",
            "_item_schema": {
                "max_tonnage": "float|null — upper GT/tonnage bound",
                "max_value": "float|null — upper bound for non-tonnage tiers (days, etc.)",
                "base_fee": "float|null — flat fee for this band",
                "base_fee_by_port": "dict[port_id→float]|null — port-specific base fees",
                "rate_per_unit_above": "float|null — rate per unit above previous band",
                "rate_per_100_tons": "float|null — rate per 100 GT in this band",
                "rate_per_100_tons_above": "dict[port_id→float]|null — port-specific rates",
                "craft_units": "float|null — tugs allocated in this band",
            },
            "value": None,
        },

        # ── For multiple_regimes (light dues) ────────────────────────────
        "regimes": {
            "_type": "list[object]|null",
            "_description": "Named sub-calculations within a section (e.g., registered vs foreign)",
            "_item_schema": {
                "id": "string — regime identifier",
                "applies_to": "list[string] — vessel categories",
                "basis": "string — 'loa_metres' or 'gross_tonnage'",
                "period": "string|null — 'financial_year', etc.",
                "rate_per_metre": "float|null",
                "rate_per_100_tons": "float|null",
                "divisor": "float|null",
                "validity": "string — when the rate applies",
                "conditions": "list[string]|null",
                "time_limits_days": "int|null",
                "territorial_limits_nm": "int|null",
            },
            "value": None,
        },

        # ── For per_service (pilotage, berthing, running_lines) ──────────
        "port_rates": {
            "_type": "dict[port_id→object]|null",
            "_description": "Port-specific base_fee + rate_per_100_tons",
            "_item_schema": {
                "base_fee": "float",
                "rate_per_100_tons": "float|null",
                "rate_per_gt": "float|null",
            },
            "value": None,
        },

        # ── For per_unit with port overrides (VTS) ───────────────────────
        "port_overrides": {
            "_type": "dict[port_id→object]|null",
            "_description": "Port-specific rate overrides (e.g., different rate_per_gt)",
            "value": None,
        },

        # ── For flat / per_teu_flat / per_leg ────────────────────────────
        "rates": {
            "_type": "dict[string→float]|null",
            "_description": "Named rate table for flat/container/coastwise charges",
            "value": None,
        },

        # ── For threshold_discount ───────────────────────────────────────
        "applies_to_charges": {
            "_type": "list[string]|null",
            "_description": "Section IDs that this discount applies to",
            "value": None,
        },
        "tiers": {
            "_type": "list[object]|null",
            "_item_schema": {
                "cargo_type": "string — CONTAINER, AUTO_CARRIERS, etc.",
                "threshold_calls": "int — minimum calls to qualify",
                "discount_pct_per_increment": "float — % discount per increment",
                "increment_calls": "int — calls per increment",
                "max_calls_for_discount": "int — cap on qualifying calls",
            },
            "value": None,
        },

        # ── For per_commodity ────────────────────────────────────────────
        "unit": {"_type": "string|null", "_example": "metric_ton", "value": None},
        "base_rates": {
            "_type": "dict[string→float]|null",
            "_description": "Default import/export rates when commodity not listed",
            "value": None,
        },
        "commodities": {
            "_type": "list[object]|null",
            "_description": "Per-commodity rate table",
            "_item_schema": {
                "name": "string — commodity name as in tariff book",
                "import": "float — import rate",
                "export": "float — export rate",
            },
            "value": None,
        },

        # ── For tiered_per_service (tugs) ────────────────────────────────
        "craft_allocation": {
            "_type": "list[object]|null",
            "_item_schema": {
                "max_tonnage": "float|null — upper GT bound",
                "craft_units": "float — number of tugs allocated",
            },
            "value": None,
        },
    },

    # ── Minimum / Maximum fees ───────────────────────────────────────────
    "minimum_fee": {"_type": "float|null", "value": None},
    "maximum_fee": {"_type": "float|null", "value": None},

    # ── Reductions ───────────────────────────────────────────────────────
    "reductions": {
        "_type": "list[object]|null",
        "_description": "Percentage discounts with conditions. Extracted from 'Reductions' or 'Rebates' tables.",
        "_item_schema": {
            "id": "string — reduction identifier (snake_case)",
            "percentage": "float — discount percentage (e.g., 35)",
            "description": "string — human-readable reason",
            "conditions": "list[string] — when this reduction applies",
            "stackable": "boolean — can combine with other reductions?",
            "not_stackable_with": "list[string]|null — IDs of incompatible reductions",
            "max_total_pct": "float|null — cap on combined reduction percentage",
            "applies_to": "string|null — 'incremental_fee_only' etc.",
        },
        "value": None,
    },

    # ── Surcharges ───────────────────────────────────────────────────────
    "surcharges": {
        "_type": "list[object]|null",
        "_description": "Additional percentage charges under specific conditions.",
        "_item_schema": {
            "percentage": "float — surcharge percentage (e.g., 50)",
            "conditions": "list[string] — when this surcharge applies",
            "per_extra_tug": "boolean|null",
            "port_id": "string|null — if port-specific",
            "applies_to": "string|null — scope of surcharge",
        },
        "value": None,
    },

    # ── Exemptions ───────────────────────────────────────────────────────
    "exemptions": {
        "_type": "list[object]|null",
        "_description": "Conditions under which the charge is fully waived.",
        "_item_schema": {
            "conditions": "list[string] — exemption conditions",
            "note": "string|null",
        },
        "value": None,
    },

    # ── Additional ───────────────────────────────────────────────────────
    "note": {
        "_type": "string|null",
        "_description": "Important notes, caveats, or special rules mentioned in the text",
        "value": None,
    },
    "special": {
        "_type": "dict|null",
        "_description": "Section-specific extra fields (delay fees, preparation fees, etc.)",
        "value": None,
    },

    # ── Section-specific optional fields ─────────────────────────────────
    "delay_fee_per_tug_per_half_hour": {"_type": "float|null", "value": None},
    "minimum_fee_small_pleasure_other_than_registered": {"_type": "float|null", "value": None},
    "free_period_cargo_working_hours_before": {"_type": "float|null", "value": None},
    "free_period_cargo_working_hours_after": {"_type": "float|null", "value": None},
}


# ─────────────────────────────────────────────────────────────────────────────
# Metadata template (filled ONCE per tariff book, not per section)
# ─────────────────────────────────────────────────────────────────────────────

METADATA_TEMPLATE: Dict[str, Any] = {
    "_instructions": (
        "Fill from the tariff book cover page / preamble. "
        "Dates must be YYYY-MM-DD format."
    ),
    "schema_version": {"_type": "string", "_default": "1.0", "value": "1.0"},
    "tariff_edition": {"_type": "string", "value": None},
    "effective_from": {"_type": "date (YYYY-MM-DD)", "value": None},
    "effective_to": {"_type": "date (YYYY-MM-DD)", "value": None},
    "currency": {"_type": "string (ISO 4217)", "value": None},
    "vat_pct": {"_type": "float", "value": None},
    "issuer": {
        "name": {"_type": "string", "value": None},
        "jurisdiction": {"_type": "string", "value": None},
        "legal_basis": {"_type": "string", "value": None},
    },
    "source_document": {
        "title": {"_type": "string", "value": None},
        "pages_total": {"_type": "integer", "value": None},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Definitions template (filled ONCE per tariff book, not per section)
# ─────────────────────────────────────────────────────────────────────────────

DEFINITIONS_TEMPLATE: Dict[str, Any] = {
    "_instructions": (
        "Fill from the Definitions / General section of the tariff book."
    ),
    "tonnage": {
        "convention": {"_type": "string", "value": None},
        "unit": {"_type": "string", "_allowed": ["gross_tonnage", "net_tonnage"], "value": None},
        "convert_to_cubic_metres": {"_type": "boolean", "value": False},
        "conversion_factor": {"_type": "float|null", "value": None},
        "fallback_source": {"_type": "string|null", "value": None},
    },
    "vessel_types": {
        "_type": "list[object]",
        "_item_schema": {
            "id": "string — snake_case identifier",
            "description": "string — definition from tariff book",
            "passenger_threshold": "int|null",
        },
        "value": [],
    },
    "working_hours": {
        "_type": "dict[port_or_default→object]",
        "_item_schema": {
            "description": "string",
            "days": "string — 'all', 'weekdays', etc.",
            "start": "string — HH:MM",
            "end": "string — HH:MM",
        },
        "value": {},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: strip metadata keys (keys starting with _) to get clean JSON
# ─────────────────────────────────────────────────────────────────────────────

def strip_meta(template: Dict[str, Any]) -> Dict[str, Any]:
    """Remove _instructions, _type, _description, _example, _allowed, _item_schema, _default
    from the template, keeping only the 'value' fields for LLM output."""
    result = {}
    for key, val in template.items():
        if key.startswith("_"):
            continue
        if isinstance(val, dict):
            if "value" in val and "_type" in val:
                # Leaf field — extract just the value slot
                result[key] = val["value"]
            else:
                # Nested object — recurse
                result[key] = strip_meta(val)
        else:
            result[key] = val
    return result


def get_section_template_for_llm() -> str:
    """Return the section template as a JSON string the LLM fills in."""
    return json.dumps(SECTION_TEMPLATE, indent=2)


def get_clean_section_template() -> Dict[str, Any]:
    """Return the section template with only value slots (what the LLM fills)."""
    return strip_meta(SECTION_TEMPLATE)


def get_metadata_template_for_llm() -> str:
    """Return the metadata template as a JSON string the LLM fills in."""
    return json.dumps(METADATA_TEMPLATE, indent=2)


def get_definitions_template_for_llm() -> str:
    """Return the definitions template as a JSON string the LLM fills in."""
    return json.dumps(DEFINITIONS_TEMPLATE, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Prompt builder — constructs the full prompt for one section extraction
# ─────────────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a tariff extraction specialist. You are given the extracted text of ONE
section from a port tariff book. Your job is to fill in the JSON template with
values found in the text.

RULES:
1. ONLY use values explicitly stated in the provided text. Never infer or hallucinate.
2. All monetary values must be numbers (float), never strings with currency symbols.
3. All percentages must be numbers (e.g., 35 not "35%").
4. Use null for any field where the text does not provide a value.
5. Condition strings must be lowercase_snake_case descriptors of the condition.
6. Port IDs must be lowercase_snake_case: richards_bay, durban, east_london,
   port_elizabeth, ngqura, mossel_bay, cape_town, saldanha.
7. Return ONLY valid JSON. No commentary, no markdown fences, no explanation.
8. The "type" field in calculation MUST be one of the allowed values listed.
9. For commodity tables: extract EVERY row. Do not summarize or skip.
10. For bands/tiers: extract EVERY band boundary. Preserve the upper limits exactly.
"""


def build_section_extraction_prompt(
    section_text: str,
    page_number: int,
    section_number: str,
) -> str:
    """Build the full user prompt for extracting one section."""
    template = get_section_template_for_llm()

    return f"""\
## Tariff Section Text (Page {page_number}, Section {section_number})

{section_text}

---

## JSON Template to Fill

{template}

---

Fill every field in the template from the section text above. Return ONLY the
filled JSON object. Use null for fields not mentioned in the text. Every monetary
value must be a number. Every percentage must be a number.
"""


def build_metadata_extraction_prompt(preamble_text: str) -> str:
    """Build the prompt for extracting metadata from the tariff book preamble."""
    template = get_metadata_template_for_llm()
    return f"""\
## Tariff Book Preamble / Cover Page

{preamble_text}

---

## JSON Template to Fill

{template}

---

Fill every field from the preamble text. Return ONLY the filled JSON.
"""


def build_definitions_extraction_prompt(definitions_text: str) -> str:
    """Build the prompt for extracting shared definitions."""
    template = get_definitions_template_for_llm()
    return f"""\
## Tariff Book Definitions Section

{definitions_text}

---

## JSON Template to Fill

{template}

---

Fill every field from the definitions text. Return ONLY the filled JSON.
"""
