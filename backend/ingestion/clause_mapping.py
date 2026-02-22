"""
Clause Mapping Node.

Send fused page content to the LLM and return draft YAML rule snippets
with keys: charge_name, rate, basis, formula, citation.
"""

import logging
import time
import yaml
from typing import List, Optional

from backend.core.config import settings
from backend.core.llm_clients import get_gemini_client
from backend.models.ingestion_models import PageExtract

logger = logging.getLogger(__name__)

RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5


def _build_fused_prompt(fused_pages: List[dict]) -> str:
    """Build LLM prompt content from fused page dicts.

    Each fused page dict has ``page_number``, ``section_header``, ``elements``
    (list of {kind, text, table, y_position}).  We render them in reading order
    with tables formatted as Markdown tables.
    """
    from backend.ingestion.page_fusion import FusedPage
    parts: List[str] = []
    for fp_dict in fused_pages:
        # Reconstruct FusedPage to use its .to_markdown() method
        # FusedPage is a dataclass so we can rebuild from dict
        try:
            fp = FusedPage(**{
                "page_number": fp_dict["page_number"],
                "section_header": fp_dict.get("section_header", ""),
                "has_tables": fp_dict.get("has_tables", False),
                "table_count": fp_dict.get("table_count", 0),
            })
            # Rebuild elements
            from backend.ingestion.page_fusion import FusedElement
            fp.elements = [
                FusedElement(**e) for e in fp_dict.get("elements", [])
            ]
            parts.append(fp.to_markdown())
        except Exception as exc:
            logger.warning(
                "Failed to reconstruct FusedPage for page %s: %s — using fallback",
                fp_dict.get("page_number", "?"), exc,
            )
            parts.append(f"## Page {fp_dict.get('page_number', '?')}\n(fusion error)")
    return "\n\n".join(parts)


def map_clauses_to_draft_rules(
    pages: List[PageExtract],
    tables_per_page: List[dict],
    *,
    fused_pages: Optional[List[dict]] = None,
    section_chunks: Optional[List[dict]] = None,
) -> List[dict]:
    """
    Extract tariff rules from page content via LLM.

    Priority:
      1. If ``section_chunks`` is provided, call LLM per-section for focused extraction.
      2. Else if ``fused_pages`` is provided, use the clean merged Markdown.
      3. Else fall back to raw text + tables dump.

    Args:
        pages: List of PageExtract from PDF parser.
        tables_per_page: List of table JSON per page (from table_extract node).
        fused_pages: Optional list of fused page dicts (from page_fusion node).
        section_chunks: Optional list of SectionChunk dicts (from section_chunker node).

    Returns:
        List of draft rule dicts, each with keys: charge_name, rate, basis, formula, citation.
    """
    # ── Best path: per-section extraction from section chunks ──
    # Minimum chunk size — skip TOC stubs and tiny headers that have no
    # extractable tariff data. Genuine sections are typically 400+ chars.
    MIN_CHUNK_CHARS = 400

    if section_chunks:
        # Filter to numbered tariff sections (skip preamble "0") with enough text
        tariff_chunks = [
            c for c in section_chunks
            if c.get("section_id", "0") != "0"
            and len(c.get("text", "").strip()) >= MIN_CHUNK_CHARS
        ]
        if tariff_chunks:
            logger.info(
                "Clause mapping using %d section chunks of %d total "
                "(filtered by section_id != '0' and text >= %d chars)",
                len(tariff_chunks), len(section_chunks), MIN_CHUNK_CHARS,
            )
            all_rules: List[dict] = []
            for chunk in tariff_chunks:
                sec_id = chunk.get("section_id", "")
                sec_name = chunk.get("section_name", "")
                sec_text = chunk.get("text", "")
                sec_pages = chunk.get("pages", [])
                logger.info("  Processing section %s: %s (%d chars, pages %s)",
                            sec_id, sec_name, len(sec_text), sec_pages)
                sections = _extract_rules_from_text(sec_text)
                # Enrich citation with section info from chunk metadata
                for s in sections:
                    cite = s.get("citation") or {}
                    if not cite.get("section"):
                        cite["section"] = f"{sec_id} {sec_name}".strip()
                    if not cite.get("page") and sec_pages:
                        cite["page"] = sec_pages[0]
                    s["citation"] = cite
                all_rules.extend(sections)
            logger.info("Clause mapping extracted %d total sections from %d section chunks",
                        len(all_rules), len(tariff_chunks))
            return all_rules

    # ── Fallback: fused pages or raw text ──
    if fused_pages:
        content_body = _build_fused_prompt(fused_pages)
    else:
        combined = []
        for i, page in enumerate(pages):
            tables = (tables_per_page[i]["tables"] if i < len(tables_per_page) else []) or []
            combined.append(
                f"--- Page {page.page_number} ---\n{page.text}\n"
                + (f"Tables: {yaml.dump(tables, default_flow_style=False)}\n" if tables else "")
            )
        content_body = "\n".join(combined)

    return _extract_rules_from_text(content_body)


# ── Core LLM extraction helper ──────────────────────────────────


_SYSTEM_PROMPT = r"""You are an expert port tariff analyst. Your task is to extract
structured tariff rules from port tariff PDF content and output them as a YAML
list under a top-level key "sections:".

Each section must have EXACTLY these 12 keys (use null/empty if not found):

  1. id           — snake_case identifier (e.g. "light_dues", "vts_charges")
  2. name         — human-readable charge name (e.g. "Light Dues")
  3. description  — one-paragraph plain-English summary of the charge
  4. citation     — object with:
                      page: <int>      — PDF page number
                      section: "<str>" — section reference (e.g. "1.1", "3.3")
  5. applicability — object with:
                      payable_by: [list of payer types]
                      conditions: [list of condition strings]
                      scope: "<str>"  (e.g. "all_sa_ports")
  6. calculation  — object describing how the charge is computed:
                      type: one of [per_unit, per_unit_per_time, per_service,
                            tiered_per_service, tiered_per_100_tons_per_24h,
                            multiple_regimes, threshold_discount, flat, formula,
                            per_commodity_per_ton, per_commodity_per_kilolitre,
                            per_teu_flat, per_leg]
                      basis: "<str>"  (e.g. "gross_tonnage", "cubic_meters", "loa", "commodity_tonnage")
                      ... plus type-specific fields (rate, divisor, bands,
                          regimes, port_rates, port_overrides, etc.)
  7. surcharges   — list of surcharge objects:
                      - percentage: <int>
                        conditions: [list of condition strings]
                      (omit if none)
  8. exemptions   — list of exemption objects:
                      - conditions: [list of condition strings]
                        description: "<str>"  (optional)
                      (omit if none)
  9. minimum_fee  — number or null
 10. maximum_fee  — number or null
 11. special      — dict of section-specific extras (e.g. delay fees,
                     misc services, special port rules). Use null if none.
 12. note         — free-text note string. Use "" if none.

IMPORTANT RULES:
- Extract ALL numeric rates, fees, and amounts EXACTLY as stated in the PDF.
- Use snake_case for all keys and enum values.
- "divisor: 100" means the rate is per 100 GT (divide GT by 100, multiply by rate).
- For port-specific rates, use port_rates or port_overrides dict keyed by
  lowercase port name (richards_bay, durban, port_elizabeth, ngqura, cape_town,
  saldanha, east_london, mossel_bay, other).
- For tiered/banded calculations, list bands from lowest to highest max_tonnage.
  Use null for the last band's max_tonnage (unlimited).
- For conditions, use snake_case descriptive strings (e.g. "outside_working_hours",
  "saps_sandf", "bonafide_coaster", "passenger_vessel").
- Output ONLY valid YAML. No commentary outside the YAML block.

--- FEW-SHOT EXAMPLES FROM GOLDEN YAML ---

EXAMPLE 1 (simple per_unit with minimum_fee and exemptions):
```yaml
sections:
  - id: vts_charges
    name: "VTS Dues"
    description: >
      Vessel Traffic Services charges per GT per port call. Higher rate at
      Durban and Saldanha Bay. Minimum fee applies.
    citation:
      page: 7
      section: "2.1"
    applicability:
      payable_by: ["vessel_owner", "vessel_agent"]
      conditions: ["vessel_calling_port_under_authority_control"]
      scope: "all_sa_ports"
    calculation:
      type: per_unit
      basis: "gross_tonnage"
      rate_per_gt: 0.36
      per: "port_call"
      port_overrides:
        durban:
          rate_per_gt: 0.54
        saldanha:
          rate_per_gt: 0.54
    minimum_fee: 231.4
    maximum_fee: null
    surcharges: []
    exemptions:
      - conditions: ["saps_sandf"]
      - conditions: ["samsa"]
      - conditions: ["sa_medical_research"]
      - conditions: ["return_from_anchorage_harbour_master_order"]
      - conditions: ["section_4_2_small_pleasure"]
    special: null
    note: ""
```

EXAMPLE 2 (per_service with port_rates and surcharges):
```yaml
sections:
  - id: pilotage
    name: "Pilotage Dues"
    description: >
      Compulsory pilotage at all ports. Per service (entering/leaving):
      base fee + per 100 tons GT. Port-specific rates.
    citation:
      page: 7
      section: "3.3"
    applicability:
      payable_by: ["vessel_owner", "vessel_agent"]
      conditions: ["pilotage_compulsory_at_all_ports"]
      scope: "all_sa_ports"
    calculation:
      type: per_service
      basis: "gross_tonnage"
      divisor: 100
      per: "service"
      port_rates:
        richards_bay:
          base_fee: 30960.46
          rate_per_100_tons: 10.93
        durban:
          base_fee: 18608.61
          rate_per_100_tons: 9.72
        cape_town:
          base_fee: 6342.39
          rate_per_100_tons: 10.20
        saldanha:
          base_fee: 9673.57
          rate_per_100_tons: 13.66
        other:
          base_fee: 6547.45
          rate_per_100_tons: 10.49
    minimum_fee: null
    maximum_fee: null
    surcharges:
      - percentage: 50
        conditions: ["outside_working_hours"]
      - percentage: 50
        conditions: ["vessel_not_ready_30_min_after_notified_time"]
      - percentage: 50
        conditions: ["cancellation_within_30_min_pilot_not_boarded"]
    exemptions:
      - conditions: ["saps_sandf_except_on_request"]
    special:
      saldanha_plo_tanker_per_hour: 886.20
    note: ""
```

EXAMPLE 3 (multiple_regimes with exemptions and note):
```yaml
sections:
  - id: light_dues
    name: "Light Dues"
    description: >
      Light dues on vessels entering SA waters. Two regimes: (1) registered
      vessels at their registered port — per metre LOA per financial year;
      (2) all other vessels — per 100 GT, valid first to last SA port.
    citation:
      page: 5
      section: "1.1"
    applicability:
      payable_by: ["vessel_owner", "vessel_agent"]
      scope: "all_sa_ports"
    calculation:
      type: multiple_regimes
      regimes:
        - id: registered_port
          applies_to: ["self_propelled", "deat_licensed_at_registered_port"]
          basis: "loa_metres"
          period: "financial_year"
          rate_per_metre: 24.64
        - id: all_other_vessels
          applies_to: ["all_other"]
          basis: "gross_tonnage"
          divisor: 100
          rate_per_100_tons: 117.08
          conditions: ["does_not_proceed_beyond_sa_coastline"]
          time_limits_days: 60
    minimum_fee: null
    maximum_fee: null
    surcharges: []
    exemptions:
      - id: full_exemption
        conditions: ["saps_sandf"]
        description: "SAPS and SANDF vessels"
      - id: samsa_exemption
        conditions: ["samsa"]
        description: "SAMSA vessels"
      - id: non_selfpropelled_small_pleasure
        conditions: ["non_self_propelled_small_pleasure_not_gain"]
        description: "Non-self-propelled small and pleasure vessels not used for gain"
    special: null
    note: >
      Coaster Light Dues raised on monthly basis per special agreement.
      From foreign port: full Light Dues at first SA port.
```

--- END EXAMPLES ---

Now extract tariff rules from the following content. Output ONLY valid YAML
with a top-level "sections:" key containing a list of section objects.
"""


def _extract_rules_from_text(content_body: str) -> List[dict]:
    """Call Gemini to extract tariff rules from text content. Return list of rule dicts."""
    client = get_gemini_client()
    model = settings.GEMINI_MODEL

    prompt = _SYSTEM_PROMPT + "\nContent:\n" + content_body

    content = ""
    for attempt in range(1, RETRY_ATTEMPTS + 2):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4096,
                temperature=0.2,
            )
            content = (response.choices[0].message.content or "").strip()
            break
        except Exception as exc:
            if attempt <= RETRY_ATTEMPTS:
                logger.warning(
                    "Clause mapping LLM call failed (attempt %d/%d): %s — retrying in %ds",
                    attempt, RETRY_ATTEMPTS + 1, exc, RETRY_DELAY_SECONDS,
                )
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error("Clause mapping LLM call failed after %d attempts: %s", attempt, exc)
                return []

    return _parse_yaml_rules(content)


def _parse_yaml_rules(content: str) -> List[dict]:
    """Parse YAML section list from LLM response. Handles code fences & truncation.

    Accepts either ``sections:`` (new schema) or ``rules:`` (legacy) top-level key.
    Each section dict is passed through as-is (full TariffSection-compatible structure).
    """
    def _extract_sections(data) -> List[dict]:
        """Pull the section/rule list from parsed YAML data."""
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Try new key first, then legacy
            items = data.get("sections") or data.get("rules") or []
            if isinstance(items, list):
                return items
        return []

    def _validate_section(s: dict) -> bool:
        """Minimal check: must have id or name."""
        return isinstance(s, dict) and bool(s.get("id") or s.get("name") or s.get("charge_name"))

    try:
        if "```yaml" in content:
            content = content.split("```yaml")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        data = yaml.safe_load(content)
        sections = _extract_sections(data)

        if not sections:
            logger.warning("YAML parse returned no sections, trying truncation salvage")
            lines = content.rstrip().rsplit("\n", 1)[0]
            data = yaml.safe_load(lines)
            sections = _extract_sections(data)
            if not sections:
                return []

        out = [s for s in sections if _validate_section(s)]
        logger.info("Extracted %d sections from LLM response", len(out))
        return out

    except yaml.YAMLError as exc:
        logger.warning("YAML parse error: %s — trying truncation salvage", exc)
        try:
            trimmed = content.rstrip()
            data = None
            for _ in range(8):
                trimmed = trimmed.rsplit("\n", 1)[0]
                try:
                    data = yaml.safe_load(trimmed)
                except yaml.YAMLError:
                    continue
                if data:
                    break
            sections = _extract_sections(data)
            out = [s for s in sections if _validate_section(s)]
            if out:
                logger.info("Salvaged %d sections from truncated YAML", len(out))
                return out
        except Exception:
            pass
        logger.error("Failed to parse YAML from clause mapping response: %s", exc)
        return []