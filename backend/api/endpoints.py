"""
API — /calculate, /chat, /citation, /ingest, /audit
==================================================
- /calculate   : Accepts CalculationRequest, returns breakdown + VAT + audit_id
- /chat        : NL query → extract fields → calculate → ChatResponse
- /citation/*  : Lookup charge citation and extract PDF page
- /ingest      : Trigger ingestion DAG (stub if DAG unavailable)
- /audit/{id}  : Retrieve audit log entry
- 422 handler  : Clear validation messages for bad input
"""
import json
import re
import uuid
import logging
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, UploadFile, Query, Response
from fastapi.responses import FileResponse

from backend.core.audit_store import audit_store
from backend.core.chat_log import ChatInteraction, get_chat_log_store
from backend.engine.tariff_engine import tariff_engine
from backend.models.schemas import (
    CalculationRequest,
    CalculationResponse,
    ChatRequest,
    ChatResponse,
    CitationResponse,
    AuditResponse,
    AuditSummary,
)
from backend.services.citation_service import citation_service
from backend.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _persist_audit(
    request: CalculationRequest,
    breakdown: list,
    total_zar: float,
    *,
    extra_input_data: dict | None = None,
) -> int:
    """Persist audit record to JSONL file and return audit id."""
    input_data = request.model_dump(mode="json")
    if extra_input_data:
        input_data = {**input_data, **extra_input_data}
    return audit_store.append(
        vessel_name=request.vessel_metadata.name,
        imo_number=request.technical_specs.imo_number,
        input_data=input_data,
        output_data=[
            b.model_dump(mode="json") if hasattr(b, "model_dump") else b
            for b in breakdown
        ],
        tariff_version=tariff_engine.version,
    )


def _vat_info(total_zar: float) -> dict:
    """Compute VAT amount and total with VAT."""
    vat_rate = 0.15
    if tariff_engine.ruleset and tariff_engine.ruleset.metadata.vat_pct:
        vat_rate = tariff_engine.ruleset.metadata.vat_pct / 100
    vat_amount = round(total_zar * vat_rate, 2)
    total_with_vat = round(total_zar + vat_amount, 2)
    return {"vat_amount": vat_amount, "total_with_vat": total_with_vat}


# ══════════════════════════════════════════════════════════════════════════════
# POST /calculate
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/calculate", response_model=CalculationResponse)
def calculate_tariff(request: CalculationRequest):
    """
    Accept CalculationRequest, run tariff engine, persist audit,
    return breakdown with VAT and audit_id.
    """
    if tariff_engine.ruleset is None:
        raise HTTPException(
            status_code=503,
            detail="Tariff rules are not loaded. Check /health for details.",
        )

    breakdown = tariff_engine.calculate(request)

    if not breakdown:
        raise HTTPException(
            status_code=422,
            detail=(
                "No applicable charges found for the given vessel and port. "
                "Verify vessel_type, gross_tonnage, and port_id are correct."
            ),
        )

    total_zar = round(sum(item.result for item in breakdown), 2)
    vat = _vat_info(total_zar)

    audit_id = _persist_audit(request, breakdown, total_zar)

    return CalculationResponse(
        total_zar=total_zar,
        vat_amount=vat["vat_amount"],
        total_with_vat=vat["total_with_vat"],
        currency="ZAR",
        breakdown=breakdown,
        audit_id=audit_id,
        tariff_version=tariff_engine.version,
    )


@router.get("/chat/status")
def chat_status():
    """
    Check whether chat mode is ready (i.e. a Gemini API key is configured server-side).
    The frontend uses this to decide whether to show the API key input.
    """
    has_key = bool(settings.GEMINI_API_KEY)
    return {
        "gemini_configured": has_key,
        "model": settings.GEMINI_CHAT_MODEL,
        "message": (
            "Gemini API key is configured. Chat mode is ready."
            if has_key
            else "No server-side Gemini API key. Please provide your own key to use chat mode."
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# GET /config — form config derived from the loaded YAML
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/config")
def get_config():
    """
    Return valid ports, vessel_types, and purposes extracted from the
    loaded TariffRuleset so the frontend can fetch them dynamically.
    """
    return tariff_engine.get_form_config()


# ══════════════════════════════════════════════════════════════════════════════
# POST /chat  (NL → Gemini Flash extraction → calculate)
# ══════════════════════════════════════════════════════════════════════════════


def _build_vessel_type_map() -> dict[str, str]:
    """Build display-label → YAML id map from the loaded ruleset."""
    cfg = tariff_engine.get_form_config()
    return {vt["label"]: vt["id"] for vt in cfg["vessel_types"]}


def _build_extraction_prompt() -> str:
    """Build the Gemini extraction prompt from the loaded ruleset config."""
    cfg = tariff_engine.get_form_config()
    vt_labels = ", ".join(vt["label"] for vt in cfg["vessel_types"])
    port_ids = ", ".join(cfg["ports"].keys())
    return (
        "You are MARC, a South African maritime port tariff calculator assistant. "
        "Your ONLY purpose is to help users calculate port dues, berth charges, "
        "cargo dues, light dues, pilotage, tugs, and related maritime tariff fees "
        "for South African ports.\n\n"
        "STEP 1 — RELEVANCE CHECK:\n"
        "First, decide whether the user's message is a request to calculate "
        "port tariff charges for a vessel visiting a South African port. "
        "The message should mention or imply at least some of: a vessel type, "
        "gross tonnage (GT), a port name, days alongside, or a cargo/tariff context.\n\n"
        "If the message is clearly OFF-TOPIC (e.g. casual chat, greetings, jokes, "
        "unrelated questions, gibberish, or random text), respond with ONLY this JSON:\n"
        '{"off_topic": true, "message": "<a short, friendly reply explaining that '
        "you are a port tariff calculator and encouraging the user to ask about "
        "port dues — include an example query like: 'Calculate dues for a 51300 GT "
        "bulk carrier at Durban for 3 days'.>\"}\n\n"
        "STEP 2 — EXTRACTION (only if the message IS relevant):\n"
        "Extract the following fields from the user's message. "
        "Return ONLY a single JSON object with these keys:\n"
        f"  vessel_type (string — one of: {vt_labels}),\n"
        "  gross_tonnage (number — the vessel's GT),\n"
        f"  port_id (string — one of: {port_ids}),\n"
        "  days_alongside (number — days the vessel stays at berth),\n"
        "  loa_meters (number — length overall in metres),\n"
        "  vessel_name (string — name of the vessel),\n"
        "  num_operations (number — pilotage/tug operations, default 2),\n"
        "  num_holds (number — cargo holds, default 7).\n"
        "Use null for any field not mentioned in the message. "
        "Do NOT guess or invent values — only extract what the user explicitly states.\n\n"
        "IMPORTANT: Your response must be ONLY valid JSON — no markdown, no explanation, "
        "no code fences. Just the raw JSON object.\n\n"
        "User message: "
    )


def _extract_via_gemini(message: str, api_key: Optional[str] = None) -> dict:
    """
    Extract vessel/operational fields from natural language using Gemini 2.5 Flash.
    This is the MANDATORY extraction path for chat mode.

    Returns a dict shaped like CalculationRequest, or raises HTTPException
    if the LLM call fails or no API key is available.

    Every call is logged to the chat interaction store (JSONL + in-memory ring buffer).
    """
    from backend.core.llm_clients import get_gemini_chat_client

    chat_store = get_chat_log_store()
    interaction_id = str(uuid.uuid4())[:12]
    system_prompt = _build_extraction_prompt()
    t0 = time.monotonic()

    def _log_interaction(
        raw_response: str | None = None,
        parsed: dict | None = None,
        error: str | None = None,
    ) -> None:
        duration_ms = round((time.monotonic() - t0) * 1000, 1)
        chat_store.record(ChatInteraction(
            interaction_id=interaction_id,
            user_message=message,
            system_prompt=system_prompt,
            raw_llm_response=raw_response,
            parsed_data=parsed,
            error=error,
            duration_ms=duration_ms,
        ))

    # Resolve API key: per-request → env-configured
    effective_key = api_key or settings.GEMINI_API_KEY
    if not effective_key:
        err = (
            "Chat mode requires a Gemini API key. "
            "Either provide your API key in the chat panel, "
            "or configure GEMINI_API_KEY in the server's .env file. "
            "Alternatively, use the Structured Form which does not require an API key."
        )
        _log_interaction(error=err)
        raise HTTPException(status_code=422, detail=err)

    client = get_gemini_chat_client(api_key=effective_key)

    try:
        r = client.chat.completions.create(
            model=settings.GEMINI_CHAT_MODEL,
            messages=[{"role": "user", "content": system_prompt + message}],
            max_tokens=400,
            temperature=0.1,
        )
        text = (r.choices[0].message.content or "").strip()
    except Exception as e:
        logger.error("Gemini Flash extraction failed: %s", e)
        err = f"Gemini API call failed: {e}. Check your API key and try again."
        _log_interaction(error=err)
        raise HTTPException(status_code=502, detail=err)

    logger.info("Gemini raw response for [%s]: %s", interaction_id, text)

    # Strip markdown code fences if present (```json ... ``` or ``` ... ```)
    cleaned = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```\s*$", "", cleaned).strip()

    # Parse the JSON from Gemini's response
    # Try the cleaned text directly first, then fall back to regex extraction
    data: dict | None = None
    try:
        data = json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        # Fall back: extract first JSON object with a balanced-brace approach
        json_match = re.search(r"\{", cleaned)
        if json_match:
            start = json_match.start()
            depth = 0
            end = start
            for i in range(start, len(cleaned)):
                if cleaned[i] == "{":
                    depth += 1
                elif cleaned[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            if depth == 0 and end > start:
                try:
                    data = json.loads(cleaned[start:end])
                except json.JSONDecodeError:
                    pass

    if data is None:
        logger.warning("Could not parse JSON from Gemini response: %s", text)
        err = (
            "I couldn't understand that as a port tariff query. "
            "I'm MARC — a South African port tariff calculator. "
            "Try something like: \"Calculate dues for a 51300 GT bulk carrier "
            "at Durban for 3 days.\""
        )
        _log_interaction(raw_response=text, error=err)
        raise HTTPException(status_code=422, detail=err)

    # ── Handle off-topic / ambiguous queries detected by the LLM ─────
    if data.get("off_topic"):
        err = data.get(
            "message",
            "I'm MARC — a South African port tariff calculator. "
            "Please ask me about port dues, berth charges, cargo dues, or related fees. "
            "For example: \"Calculate dues for a 51300 GT bulk carrier at Durban for 3 days.\"",
        )
        _log_interaction(raw_response=text, parsed=data, error=f"off_topic: {err}")
        raise HTTPException(status_code=422, detail=err)

    # Log the successful extraction
    _log_interaction(raw_response=text, parsed=data)

    # Build the structured output
    out: dict[str, Any] = {
        "vessel_metadata": {"name": data.get("vessel_name") or "Unknown"},
        "technical_specs": {
            "type": "Bulk Carrier",
            "vessel_type": "bulk_carrier",
            "gross_tonnage": None,
            "loa_meters": 200.0,
        },
        "operational_data": {
            "port_id": None,
            "days_alongside": 3.0,
            "arrival_time": "2024-11-15T10:00:00",
            "departure_time": "2024-11-18T10:00:00",
            "activity": "Cargo",
            "num_operations": 2,
            "num_holds": 7,
        },
    }

    # Apply extracted fields
    if data.get("gross_tonnage") is not None:
        out["technical_specs"]["gross_tonnage"] = float(data["gross_tonnage"])

    if data.get("vessel_type"):
        vt_display = str(data["vessel_type"]).title()
        out["technical_specs"]["type"] = vt_display
        out["technical_specs"]["vessel_type"] = _build_vessel_type_map().get(
            vt_display, vt_display.lower().replace(" ", "_")
        )

    if data.get("port_id"):
        out["operational_data"]["port_id"] = str(data["port_id"]).lower().replace(" ", "_")

    if data.get("days_alongside") is not None:
        out["operational_data"]["days_alongside"] = float(data["days_alongside"])

    if data.get("loa_meters") is not None:
        out["technical_specs"]["loa_meters"] = float(data["loa_meters"])

    if data.get("num_operations") is not None:
        out["operational_data"]["num_operations"] = int(data["num_operations"])

    if data.get("num_holds") is not None:
        out["operational_data"]["num_holds"] = int(data["num_holds"])

    return out


@router.post("/chat", response_model=ChatResponse)
def chat(body: ChatRequest):
    """
    Accept a natural-language message, extract structured fields via Gemini 2.5 Flash,
    run tariff engine, return ChatResponse with breakdown + extracted fields.

    Requires a Gemini API key — either configured server-side (GEMINI_API_KEY env var)
    or provided per-request in the request body.
    """
    payload = _extract_via_gemini(body.message, api_key=body.api_key)

    # ── Validate that critical fields were actually extracted ─────────
    missing: list[str] = []
    gt = payload.get("technical_specs", {}).get("gross_tonnage")
    port = payload.get("operational_data", {}).get("port_id")

    if gt is None:
        missing.append("gross_tonnage (e.g. '51300 GT')")
    if port is None:
        missing.append("port (e.g. 'at Durban', 'in Cape Town')")

    if missing:
        raise HTTPException(
            status_code=422,
            detail=(
                "Could not extract required fields from your message: "
                + ", ".join(missing)
                + ". Please include the vessel's gross tonnage and port name."
            ),
        )

    try:
        request = CalculationRequest(**payload)
    except Exception as e:
        raise HTTPException(
            status_code=422,
            detail=f"Could not build calculation request from message: {e}",
        )

    breakdown = tariff_engine.calculate(request)
    total_zar = round(sum(item.result for item in breakdown), 2)
    vat = _vat_info(total_zar)

    audit_id = _persist_audit(
        request, breakdown, total_zar,
        extra_input_data={"_chat_message": body.message},
    )

    return ChatResponse(
        total_zar=total_zar,
        vat_amount=vat["vat_amount"],
        total_with_vat=vat["total_with_vat"],
        currency="ZAR",
        breakdown=[b for b in breakdown],
        audit_id=audit_id,
        tariff_version=tariff_engine.version,
        extracted_fields=payload,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Citation Endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/citation/{charge_name}", response_model=CitationResponse)
def get_citation(charge_name: str):
    """
    Lookup citation for a charge by its display name (e.g. "Light Dues").
    Returns page number and section reference from the tariff book.
    """
    citation = citation_service.get(charge_name)
    if citation is None:
        return CitationResponse(
            charge_name=charge_name,
            citation=None,
            found=False,
        )
    return CitationResponse(
        charge_name=charge_name,
        citation=citation,
        found=True,
    )


@router.get("/citation/{charge_name}/page")
def get_citation_page(
    charge_name: str,
    pdf_filename: str = Query("Port Tariff.pdf", description="PDF file name in storage/pdfs"),
):
    """
    Return the raw PDF page bytes for a charge's citation.
    Useful for frontend PDF.js rendering with page highlighting.
    """
    citation = citation_service.get(charge_name)
    if citation is None:
        raise HTTPException(status_code=404, detail=f"No citation found for '{charge_name}'")

    page_bytes = citation_service.get_page_bytes(pdf_filename, citation.page)
    if page_bytes is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not extract page {citation.page} from '{pdf_filename}'"
        )

    return Response(
        content=page_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{charge_name}_page_{citation.page}.pdf"',
            "X-Page-Number": str(citation.page),
            "X-Section": citation.section,
        },
    )


@router.get("/citations")
def list_citations():
    """Return all available charge citations."""
    return {
        name: {"page": cit.page, "section": cit.section}
        for name, cit in citation_service._by_name.items()
    }


@router.get("/tariff-pdf")
def get_tariff_pdf(
    filename: str = Query("Port Tariff.pdf", description="PDF file name in storage/pdfs"),
):
    """
    Stream the full tariff PDF for frontend PDF viewer (e.g. pdf.js).
    Enables loading the document once and navigating to citation pages.
    """
    path = Path(settings.PDF_DIR) / filename
    if not path.is_file():
        path = Path(settings.PDF_DIR) / Path(filename).name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"Tariff PDF not found: {filename}")
    return FileResponse(
        path,
        media_type="application/pdf",
        filename=path.name,
    )


# ══════════════════════════════════════════════════════════════════════════════
# POST /ingest
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/ingest")
def ingest_pdf(
    file_path: Optional[str] = None,
    file: Optional[UploadFile] = None,
):
    """
    Accept PDF via file_path param or file upload; trigger ingestion DAG.
    Returns 200 with job status. If the ingestion DAG is not available,
    returns a stub response indicating the offline pipeline should be used.
    """
    project_root = Path(__file__).resolve().parents[2]
    pdf_path: Optional[str] = None

    if file_path:
        p = Path(file_path)
        if not p.is_absolute():
            p = project_root / file_path
        pdf_path = str(p)
    if file and file.filename and not pdf_path:
        import tempfile
        suffix = Path(file.filename).suffix or ".pdf"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            content = file.file.read()
            tmp.write(content)
            pdf_path = tmp.name

    if not pdf_path:
        raise HTTPException(
            status_code=400,
            detail="Provide either file_path (query param) or file (multipart upload)",
        )

    # Try to import and run the ingestion DAG; graceful fallback if unavailable
    try:
        from backend.ingestion.dag import run_ingestion
        result = run_ingestion(pdf_path)
        return {
            "status": getattr(result, "status", "completed"),
            "job_id": getattr(result, "job_id", None) or str(uuid.uuid4()),
            "rules_count": getattr(result, "rules_count", None),
            "message": getattr(result, "message", None),
        }
    except (ImportError, ModuleNotFoundError) as e:
        logger.warning(f"Ingestion DAG not available ({e}) — returning stub response")
        return {
            "status": "pending",
            "job_id": str(uuid.uuid4()),
            "message": (
                "Ingestion DAG is not yet wired. "
                "Use the offline pipeline: python -m backend.ingestion.dag <pdf_path>"
            ),
        }
    except Exception as e:
        # Catch any DAG runtime errors (e.g. missing langgraph) gracefully
        err_msg = str(e)
        if "langgraph" in err_msg.lower() or "No module named" in err_msg:
            logger.warning(f"DAG dependency missing ({e}) — returning stub response")
            return {
                "status": "pending",
                "job_id": str(uuid.uuid4()),
                "message": f"Ingestion DAG dependency unavailable: {e}",
            }
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# GET /audit  &  GET /audit/{id}
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/audit", response_model=list[AuditSummary])
def list_audits(limit: int = 50):
    """Return recent audit summaries (id, vessel_name, timestamp, user_message) for the dropdown."""
    records = audit_store.list_recent(limit)
    return [
        AuditSummary(
            id=r["id"],
            vessel_name=r["vessel_name"],
            timestamp=r.get("timestamp"),
            user_message=(r.get("input_data") or {}).get("_chat_message"),
        )
        for r in records
    ]


@router.get("/audit/{audit_id}", response_model=AuditResponse)
def get_audit(audit_id: int):
    """Return audit log entry for a calculation."""
    log = audit_store.get(audit_id)
    if not log:
        raise HTTPException(status_code=404, detail="Audit log not found")
    return AuditResponse(
        id=log["id"],
        vessel_name=log["vessel_name"],
        imo_number=log.get("imo_number"),
        timestamp=log.get("timestamp"),
        input_data=log.get("input_data"),
        output_data=log.get("output_data"),
        tariff_version=log.get("tariff_version"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Developer Prompt Panel — GET /prompts/config  &  GET /prompts
# Controlled by ENABLE_PROMPT_PANEL in .env
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/prompts/config")
def prompts_config():
    """Tell the frontend whether the prompt panel is enabled."""
    return {"enabled": settings.ENABLE_PROMPT_PANEL}


@router.get("/prompts")
def list_prompts(limit: int = Query(50, ge=1, le=200)):
    """Return recent chat interactions for developer review.

    Only available when ENABLE_PROMPT_PANEL=true in .env.
    """
    store = get_chat_log_store()
    if not store.panel_enabled:
        raise HTTPException(
            status_code=404,
            detail="Prompt panel is not enabled. Set ENABLE_PROMPT_PANEL=true in .env.",
        )
    return store.get_recent(limit=limit)
