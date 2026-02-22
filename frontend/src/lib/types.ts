/** API types aligned with backend/models/schemas.py */

export interface Citation {
  page: number;
  section: string;
  bounding_box?: number[] | null;
}

export interface ChargeBreakdown {
  charge: string;
  basis: number;
  rate: number;
  rate_detail?: Record<string, unknown> | null;
  formula: string;
  result: number;
  citation: Citation;
}

export interface CalculationResponse {
  total_zar: number;
  vat_amount?: number | null;
  total_with_vat?: number | null;
  currency: string;
  tariff_version: string;
  breakdown: ChargeBreakdown[];
  audit_id?: number | null;
}

export interface CalculationRequest {
  vessel_metadata: {
    name: string;
    built_year?: number | null;
    flag?: string | null;
    classification_society?: string | null;
    call_sign?: string | null;
  };
  technical_specs: {
    imo_number?: string | null;
    type: string;
    vessel_type?: string;
    dwt?: number | null;
    gross_tonnage: number;
    net_tonnage?: number | null;
    loa_meters: number;
    beam_meters?: number | null;
    moulded_depth_meters?: number | null;
    lbp_meters?: number | null;
    draft_sw_s_w_t?: number[] | null;
    suez_gt?: number | null;
    suez_nt?: number | null;
  };
  operational_data: {
    port_id?: string;
    cargo_quantity_mt?: number | null;
    cargo_type?: string;
    commodity?: string;
    days_alongside: number;
    arrival_time: string;
    departure_time: string;
    activity: string;
    purpose?: string;
    num_operations?: number | null;
    num_holds?: number | null;
    is_cargo_working?: boolean;
    certifications?: string[];
    is_coaster?: boolean;
    num_tug_operations?: number | null;
  };
}

export interface ChatResponse extends CalculationResponse {
  extracted_fields?: Record<string, unknown> | null;
}

export interface AuditResponse {
  id: number;
  vessel_name: string;
  imo_number?: string | null;
  timestamp?: string | null;
  input_data?: Record<string, unknown> | null;
  output_data?: unknown;
  tariff_version?: string | null;
}

/** Lightweight audit summary for the dropdown selector. */
export interface AuditSummary {
  id: number;
  vessel_name: string;
  timestamp?: string | null;
  /** Document Q&A question when this audit came from chat. */
  user_message?: string | null;
}

export interface ChatStatus {
  gemini_configured: boolean;
  model: string;
  message: string;
}

/** Form configuration derived from the backend YAML ruleset. */
export interface FormConfig {
  /** port_id → display label, e.g. { durban: "Durban", ngqura: "Ngqura" } */
  ports: Record<string, string>;
  /** Vessel types, e.g. [{ id: "bulk_carrier", label: "Bulk Carrier" }] */
  vessel_types: { id: string; label: string }[];
  /** Visit purposes, e.g. [{ id: "cargo_loading", label: "Cargo Loading" }] */
  purposes: { id: string; label: string }[];
}

/** A single chat interaction log entry (developer prompt panel). */
export interface PromptLog {
  id: string;
  timestamp: string;
  user_message: string;
  system_prompt: string;
  raw_llm_response: string | null;
  parsed_data: Record<string, unknown> | null;
  error: string | null;
  duration_ms: number;
}

/** Whether the developer prompt panel is enabled on the backend. */
export interface PromptsConfig {
  enabled: boolean;
}
