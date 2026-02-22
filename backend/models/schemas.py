from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


# ── Enums for request fields ─────────────────────────────────────────────

class VesselType(str, Enum):
    """Vessel type classification — must align with YAML definitions.vessel_types[].id"""
    BULK_CARRIER = "bulk_carrier"
    TANKER = "tanker"
    CONTAINER = "container"
    PASSENGER_VESSEL = "passenger_vessel"
    PLEASURE_VESSEL = "pleasure_vessel"
    SMALL_VESSEL = "small_vessel"
    FISHING_VESSEL = "fishing_vessel"
    COASTER = "coaster"
    OTHER = "other"


class VisitPurpose(str, Enum):
    """Primary purpose of the port call — drives reductions/exemptions."""
    CARGO_LOADING = "cargo_loading"
    CARGO_DISCHARGE = "cargo_discharge"
    BUNKERS_STORES_WATER = "bunkers_stores_water"
    REPAIRS = "repairs"
    CREW_CHANGE = "crew_change"
    TRANSIT = "transit"
    OTHER = "other"


# ── Request Models ───────────────────────────────────────────────────────

class VesselMetadata(BaseModel):
    name: str
    built_year: Optional[int] = None
    flag: Optional[str] = None
    classification_society: Optional[str] = None
    call_sign: Optional[str] = None

class TechnicalSpecs(BaseModel):
    imo_number: Optional[str] = None
    type: str
    vessel_type: VesselType = VesselType.OTHER
    dwt: Optional[float] = None
    gross_tonnage: float
    net_tonnage: Optional[float] = None
    loa_meters: float
    beam_meters: Optional[float] = None
    moulded_depth_meters: Optional[float] = None
    lbp_meters: Optional[float] = None
    draft_sw_s_w_t: Optional[List[float]] = None
    suez_gt: Optional[float] = None
    suez_nt: Optional[float] = None

class OperationalData(BaseModel):
    port_id: str = "other"
    cargo_quantity_mt: Optional[float] = None
    cargo_type: str = ""
    commodity: str = ""
    days_alongside: float
    arrival_time: datetime
    departure_time: datetime
    activity: str
    purpose: VisitPurpose = VisitPurpose.OTHER
    num_operations: Optional[int] = None
    num_holds: Optional[int] = None
    is_cargo_working: bool = True
    certifications: List[str] = Field(
        default_factory=list,
        description="e.g. ['double_hull', 'segregated_ballast', 'green_award']"
    )
    is_coaster: bool = False
    num_tug_operations: Optional[int] = None  # For towage: how many moves

    @property
    def stay_hours(self) -> float:
        """Total hours between arrival and departure."""
        delta = self.departure_time - self.arrival_time
        return delta.total_seconds() / 3600.0

class CalculationRequest(BaseModel):
    vessel_metadata: VesselMetadata
    technical_specs: TechnicalSpecs
    operational_data: OperationalData


# ── Response Models ──────────────────────────────────────────────────────

class Citation(BaseModel):
    page: int
    section: str
    bounding_box: Optional[List[float]] = None

class ChargeBreakdown(BaseModel):
    charge: str
    basis: float
    rate: float  # Primary rate (for simple charges)
    rate_detail: Optional[Dict[str, Any]] = Field(
        None,
        description="Structured rate info for complex charges (tiers, port rates, time-based)"
    )
    formula: str
    result: float
    citation: Citation

class CalculationResponse(BaseModel):
    total_zar: float
    vat_amount: Optional[float] = None
    total_with_vat: Optional[float] = None
    currency: str = "ZAR"
    tariff_version: str = ""
    breakdown: List[ChargeBreakdown]
    audit_id: Optional[int] = None


# ── Chat Models ──────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    """Natural-language chat request."""
    message: str = Field(..., min_length=1, description="Free-text query, e.g. 'Calculate dues for a 51300 GT bulk carrier at Durban'")
    api_key: Optional[str] = Field(None, description="Optional Gemini API key. If not set, the server's GEMINI_API_KEY env var is used.")


class ChatResponse(BaseModel):
    """Response from the /chat endpoint."""
    total_zar: float
    vat_amount: Optional[float] = None
    total_with_vat: Optional[float] = None
    currency: str = "ZAR"
    tariff_version: str = ""
    breakdown: List[ChargeBreakdown]
    audit_id: Optional[int] = None
    extracted_fields: Optional[Dict[str, Any]] = Field(
        None, description="Fields extracted from the natural-language query"
    )


# ── Citation Models ──────────────────────────────────────────────────────

class CitationResponse(BaseModel):
    """Response for citation lookup."""
    charge_name: str
    citation: Optional[Citation] = None
    found: bool = True


# ── Audit Models ─────────────────────────────────────────────────────────

class AuditResponse(BaseModel):
    """Response for GET /audit/{id}."""
    id: int
    vessel_name: str
    imo_number: Optional[str] = None
    timestamp: Optional[str] = None
    input_data: Optional[Dict[str, Any]] = None
    output_data: Optional[Any] = None
    tariff_version: Optional[str] = None


class AuditSummary(BaseModel):
    """Lightweight summary for the audit list dropdown."""
    id: int
    vessel_name: str
    timestamp: Optional[str] = None
    user_message: Optional[str] = None  # Document Q&A question when present


# ── Error Models ─────────────────────────────────────────────────────────

class ValidationErrorDetail(BaseModel):
    """Structured validation error for 422 responses."""
    field: str
    message: str
    type: str = "value_error"
