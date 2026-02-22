"""
tariff_rule.py — Pydantic models mirroring the YAML DSL.

These are NOT request/response models. They are the Python-typed mirror of
`tariff_rules_{version}.yaml`. The engine loads a YAML, parses it through
TariffRuleset, and gets typed, validated objects to evaluate.

Hierarchy:
    TariffRuleset
    ├── TariffMetadata
    │   ├── Issuer
    │   └── SourceDocument
    ├── TariffDefinitions
    │   ├── TonnageDefinition
    │   ├── VesselTypeDefinition[]
    │   ├── CargoUnitDefinition
    │   └── WorkingHoursDefinition
    └── TariffSection[]
        ├── Citation
        ├── Applicability
        ├── Calculation
        │   ├── Band[]
        │   ├── Regime[]
        │   └── PortRate (dict)
        ├── Reduction[]
        ├── Surcharge[]
        └── Exemption[]
"""

from __future__ import annotations

import yaml
from datetime import date
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────

class CalculationType(str, Enum):
    """Every calculation strategy the engine must handle."""
    PER_UNIT = "per_unit"
    PER_UNIT_PER_TIME = "per_unit_per_time"
    PER_UNIT_PER_PERIOD = "per_unit_per_period"
    TIERED = "tiered"
    TIERED_PER_SERVICE = "tiered_per_service"
    TIERED_PER_100_TONS_PER_24H = "tiered_per_100_tons_per_24h"
    TIERED_TIME = "tiered_time"
    PER_SERVICE = "per_service"
    PER_COMMODITY = "per_commodity_per_ton"
    PER_COMMODITY_PER_KL = "per_commodity_per_kilolitre"
    PER_TEU_FLAT = "per_teu_flat"
    PER_LEG = "per_leg"
    THRESHOLD_DISCOUNT = "threshold_discount"
    MULTIPLE_REGIMES = "multiple_regimes"
    FLAT = "flat"
    FORMULA = "formula"


class TonnageUnit(str, Enum):
    GROSS_TONNAGE = "gross_tonnage"
    NET_TONNAGE = "net_tonnage"
    CUBIC_METRES = "cubic_metres"


# ── Metadata ─────────────────────────────────────────────────────────────

class Issuer(BaseModel):
    name: str
    jurisdiction: str = ""
    legal_basis: str = ""


class SourceDocument(BaseModel):
    title: str = ""
    url: str = ""
    pages_total: Optional[int] = None


class TariffMetadata(BaseModel):
    schema_version: str = "1.0"
    tariff_edition: str = ""
    effective_from: date
    effective_to: date
    currency: str = Field(..., min_length=3, max_length=3, description="ISO 4217")
    vat_pct: Optional[float] = None
    issuer: Issuer
    source_document: Optional[SourceDocument] = None


# ── Definitions ──────────────────────────────────────────────────────────

class TonnageDefinition(BaseModel):
    convention: str = "Tonnage Convention 1969"
    unit: TonnageUnit = TonnageUnit.GROSS_TONNAGE
    convert_to_cubic_metres: bool = False
    conversion_factor: Optional[float] = None
    fallback_source: str = ""
    include_cargo_mass: bool = False


class VesselTypeDefinition(BaseModel):
    id: str
    description: str = ""
    passenger_threshold: Optional[int] = None


class CargoUnitException(BaseModel):
    commodity_type: str
    unit: str
    description: str = ""


class CargoUnitDefinition(BaseModel):
    unit: str = "metric_ton"
    min_quantity: float = 1
    exceptions: List[CargoUnitException] = Field(default_factory=list)


class WorkingHoursSpec(BaseModel):
    """Working hours for a single port or the default."""
    description: str = ""
    days: str = "all"
    start: str = "00:01"
    end: str = "24:00"
    # Alternate day-specific overrides (e.g., East London)
    weekdays: Optional[Dict[str, str]] = None
    saturdays: Optional[Dict[str, str]] = None


class TariffDefinitions(BaseModel):
    tonnage: TonnageDefinition = Field(default_factory=TonnageDefinition)
    vessel_types: List[VesselTypeDefinition] = Field(default_factory=list)
    unit_of_tonnage_cargo: Optional[CargoUnitDefinition] = None
    working_hours: Dict[str, WorkingHoursSpec] = Field(default_factory=dict)


# ── Section building blocks ──────────────────────────────────────────────

class Citation(BaseModel):
    page: int
    section: str = ""


class Applicability(BaseModel):
    payable_by: List[str] = Field(default_factory=list)
    conditions: List[str] = Field(default_factory=list)
    scope: str = ""
    cargo_working_vessels_only: bool = False


class Band(BaseModel):
    """A single tier/band in a tiered calculation."""
    max_value: Optional[float] = Field(None, description="Upper bound; null = unbounded")
    max_tonnage: Optional[float] = Field(None, description="Alias for max_value in tonnage context")
    base_fee: Optional[float] = None
    base_fee_by_port: Optional[Dict[str, Optional[float]]] = None
    rate_per_unit_above: Optional[float] = None
    rate_per_100_tons: Optional[float] = None
    rate_per_100_tons_above: Optional[Dict[str, Optional[float]]] = None
    craft_units: Optional[float] = None
    divisor: Optional[float] = None

    @property
    def upper_bound(self) -> Optional[float]:
        return self.max_value or self.max_tonnage


class Regime(BaseModel):
    """A named sub-calculation within a section (e.g., light dues has two regimes)."""
    id: str
    applies_to: List[str] = Field(default_factory=list)
    basis: str = ""
    period: str = ""
    rate_per_metre: Optional[float] = None
    rate_per_100_tons: Optional[float] = None
    divisor: Optional[float] = None
    validity: str = ""
    conditions: List[str] = Field(default_factory=list)
    time_limits_days: Optional[int] = None
    territorial_limits_nm: Optional[int] = None
    coastal_after_days: Optional[int] = None
    coastal_basis: str = ""


class PortRate(BaseModel):
    """Port-specific rate (base_fee + rate_per_100_tons)."""
    base_fee: Optional[float] = None
    rate_per_100_tons: Optional[float] = None
    rate_per_gt: Optional[float] = None


class IncentiveTier(BaseModel):
    """Tier for marine services incentive discount."""
    cargo_type: str
    threshold_calls: int
    discount_pct_per_increment: float = 1.0
    increment_calls: int
    max_calls_for_discount: int


class Reduction(BaseModel):
    id: str = ""
    percentage: float
    description: str = ""
    conditions: List[str] = Field(default_factory=list)
    stackable: bool = False
    not_stackable_with: List[str] = Field(default_factory=list)
    max_total_pct: Optional[float] = None
    applies_to: str = ""  # e.g. "incremental_fee_only"


class Surcharge(BaseModel):
    percentage: float
    conditions: List[str] = Field(default_factory=list)
    per_extra_tug: bool = False
    port_id: str = ""
    applies_to: str = ""  # e.g. "incremental_fee_only"


class Exemption(BaseModel):
    id: str = ""
    conditions: List[str] = Field(default_factory=list)
    description: str = ""
    note: str = ""


class CraftAllocation(BaseModel):
    """Tug allocation rule based on vessel tonnage."""
    max_tonnage: Optional[float] = None
    craft_units: float


# ── Calculation (the core polymorphic field) ─────────────────────────────

class Calculation(BaseModel):
    """
    The calculation specification for a section. The `type` field determines
    which handler the engine invokes. Additional fields are type-dependent.

    This is intentionally permissive (Optional fields) because different
    calculation types use different subsets. The engine validates at dispatch.
    """
    type: str  # CalculationType value, kept as str for forward-compat

    # Basis
    basis: Optional[Union[str, List[str]]] = None
    divisor: Optional[float] = None
    period: str = ""
    rounding: str = ""
    per: str = ""  # "port_call", "service", etc.

    # Simple rate
    rate: Optional[float] = None
    rate_per_gt: Optional[float] = None
    rate_per_100_tons: Optional[float] = None
    base_rate_per_100_tons: Optional[float] = None
    incremental_rate_per_100_tons_per_24h: Optional[float] = None
    period_rounding: str = ""

    # Tiered / banded
    bands: List[Band] = Field(default_factory=list)

    # Regime-based (e.g. light dues)
    regimes: List[Regime] = Field(default_factory=list)

    # Port-specific
    port_rates: Dict[str, PortRate] = Field(default_factory=dict)
    port_overrides: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Rates dict (for container/cargo dues)
    rates: Dict[str, float] = Field(default_factory=dict)

    # Threshold discount
    applies_to_charges: List[str] = Field(default_factory=list)
    tiers: List[IncentiveTier] = Field(default_factory=list)

    # Commodity-based
    base_rates: Dict[str, float] = Field(default_factory=dict)
    commodities: List[Dict[str, Any]] = Field(default_factory=list)
    unit: str = ""

    # Craft allocation (tugs)
    craft_allocation: List[CraftAllocation] = Field(default_factory=list)

    class Config:
        extra = "allow"  # Forward-compat: new PDFs may add fields


# ── TariffSection — the core repeated element ────────────────────────────

class TariffSection(BaseModel):
    """
    One charge category in the tariff book.
    Maps to one row in the final ChargeBreakdown output.
    """
    id: str
    name: str
    description: str = ""
    citation: Optional[Citation] = None

    applicability: Optional[Applicability] = None
    calculation: Calculation

    minimum_fee: Optional[float] = None
    maximum_fee: Optional[float] = None

    reductions: List[Reduction] = Field(default_factory=list)
    surcharges: List[Surcharge] = Field(default_factory=list)
    exemptions: List[Exemption] = Field(default_factory=list)

    # Section-specific extras (kept flexible)
    note: str = ""
    special: Dict[str, Any] = Field(default_factory=dict)

    # Tug-specific
    delay_fee_per_tug_per_half_hour: Optional[float] = None

    # Port dues specific
    minimum_fee_small_pleasure_other_than_registered: Optional[float] = None

    # Berth dues specific
    free_period_cargo_working_hours_before: Optional[float] = None
    free_period_cargo_working_hours_after: Optional[float] = None

    class Config:
        extra = "allow"  # Forward-compat


# ── TariffRuleset — the top-level YAML document ─────────────────────────

class TariffRuleset(BaseModel):
    """
    Top-level model for a tariff_rules_{version}.yaml file.

    Usage:
        with open("storage/yaml/tariff_rules_latest.yaml") as f:
            data = yaml.safe_load(f)
        ruleset = TariffRuleset(**data)
    """
    metadata: TariffMetadata
    definitions: TariffDefinitions = Field(default_factory=TariffDefinitions)
    sections: List[TariffSection] = Field(default_factory=list)

    # ── Convenience ──────────────────────────────────────────────────

    def get_section(self, section_id: str) -> Optional[TariffSection]:
        """Lookup a section by ID."""
        for s in self.sections:
            if s.id == section_id:
                return s
        return None

    def section_ids(self) -> List[str]:
        return [s.id for s in self.sections]

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "TariffRuleset":
        """Load and validate a tariff YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)
