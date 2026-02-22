"""
LangGraph DAG for Offline Ingestion.

Wires: PDF Parser → Table Extract → Page Fusion → Section Chunker → Vector Indexer
       → Clause Mapping → Schema Validation → Ingestion Eval
       → LLM Reviewer → Repair Loop (if confidence < 0.8, up to 3 retries)
       → Persist Rule.

Exposes run_ingestion(pdf_path) → IngestionResult.
"""

import logging
from pathlib import Path
from typing import Any, Literal, TypedDict

from backend.core.config import settings
from backend.models.ingestion_models import IngestionResult, PageExtract

logger = logging.getLogger(__name__)

# Pipeline config (from pipeline.yaml)
REPAIR_CONFIDENCE_THRESHOLD = 0.8
REPAIR_MAX_RETRIES = 3


# ── State schema for the graph ───────────────────────────────────────────

class IngestionState(TypedDict, total=False):
    pdf_path: str
    pages: list[Any]  # List[PageExtract]
    tables_per_page: list[dict]
    fused_pages: list[dict]  # List[FusedPage.to_dict()] — merged text + tables
    section_chunks: list[dict]  # List[SectionChunk.to_dict()] — section-wise chunks
    vector_index_info: dict     # chunk_count, index_path, metadata_path
    draft_rules: list[dict]
    validated_rules: list[dict]
    confidence: float
    eval_metrics: dict
    repair_count: int
    status: str
    message: str


# ── Node implementations (call pipeline steps) ───────────────────────────

def _node_pdf_parser(state: IngestionState) -> IngestionState:
    from backend.ingestion.pdf_parser import parse_pdf
    pdf_path = state["pdf_path"]
    pages = parse_pdf(pdf_path)
    return {**state, "pages": [p.model_dump() if hasattr(p, "model_dump") else p for p in pages]}


def _node_table_extract(state: IngestionState) -> IngestionState:
    from backend.ingestion.gemini_extract import extract_all_pages
    from backend.models.ingestion_models import PageExtract
    pdf_path = state["pdf_path"]
    pages_data = state.get("pages") or []
    pages = [PageExtract(**p) if isinstance(p, dict) else p for p in pages_data]
    extracts = extract_all_pages(pdf_path, pages=pages)
    return {**state, "tables_per_page": extracts}


def _node_page_fusion(state: IngestionState) -> IngestionState:
    from backend.ingestion.page_fusion import fuse_all_pages
    from backend.models.ingestion_models import PageExtract
    pages_data = state.get("pages") or []
    pages = [PageExtract(**p) if isinstance(p, dict) else p for p in pages_data]
    tables_per_page = state.get("tables_per_page") or []
    pdf_path = state.get("pdf_path")
    fused = fuse_all_pages(pages, tables_per_page, pdf_path=pdf_path)
    return {**state, "fused_pages": [fp.to_dict() for fp in fused]}


def _node_section_chunker(state: IngestionState) -> IngestionState:
    from backend.ingestion.section_chunker import chunk_fused_pages
    fused_pages = state.get("fused_pages") or []
    chunks = chunk_fused_pages(fused_pages)
    return {**state, "section_chunks": chunks}


def _node_vector_indexer(state: IngestionState) -> IngestionState:
    from backend.ingestion.vector_indexer import index_section_chunks
    section_chunks = state.get("section_chunks") or []
    index_info = index_section_chunks(section_chunks, rebuild=True)
    return {**state, "vector_index_info": index_info}


def _node_clause_mapping(state: IngestionState) -> IngestionState:
    from backend.ingestion.clause_mapping import map_clauses_to_draft_rules
    pages_data = state.get("pages") or []
    pages = [PageExtract(**p) if isinstance(p, dict) else p for p in pages_data]
    tables_per_page = state.get("tables_per_page") or []
    fused_pages = state.get("fused_pages")
    section_chunks = state.get("section_chunks")  # prefer section chunks if available
    draft_rules = map_clauses_to_draft_rules(
        pages, tables_per_page,
        fused_pages=fused_pages,
        section_chunks=section_chunks,
    )
    return {**state, "draft_rules": draft_rules}


def _node_schema_validation(state: IngestionState) -> IngestionState:
    from backend.ingestion.schema_validation import validate_draft_rules
    draft_rules = state.get("draft_rules") or []
    validated, _rejected = validate_draft_rules(draft_rules)
    return {**state, "validated_rules": validated}


def _node_ingestion_eval(state: IngestionState) -> IngestionState:
    from evals.ingestion_eval import eval_extracted_rules
    validated = state.get("validated_rules") or []
    getattr(settings, "YAML_DIR", None) or Path(settings.YAML_DIR)
    metrics = eval_extracted_rules(validated, golden_path=Path(settings.YAML_DIR))
    return {**state, "eval_metrics": metrics}


def _node_llm_reviewer(state: IngestionState) -> IngestionState:
    from backend.ingestion.llm_reviewer import review_draft_rules
    # Reviewer works on validated rules (or draft if no validated)
    rules = state.get("validated_rules") or state.get("draft_rules") or []
    confidence, repaired = review_draft_rules(rules)
    repair_count = state.get("repair_count", 0) + 1
    return {
        **state,
        "confidence": confidence,
        "validated_rules": repaired,
        "draft_rules": repaired,
        "repair_count": repair_count,
    }


def _node_persist(state: IngestionState) -> IngestionState:
    from backend.ingestion.persist_rule import persist_rules
    validated = state.get("validated_rules") or []
    try:
        _files, _rows = persist_rules(validated)
        status = "low_confidence" if state.get("confidence", 1.0) < REPAIR_CONFIDENCE_THRESHOLD else "success"
        return {**state, "status": status, "message": "Persisted"}
    except Exception as e:
        logger.warning("Persist failed: %s", e)
        return {**state, "status": "failed", "message": str(e)}


def _should_repair(state: IngestionState) -> Literal["repair", "persist"]:
    """Route: repair again or go to persist."""
    confidence = state.get("confidence", 0.0)
    repair_count = state.get("repair_count", 0)
    if confidence >= REPAIR_CONFIDENCE_THRESHOLD:
        return "persist"
    if repair_count >= REPAIR_MAX_RETRIES:
        logger.warning("Repair max retries (%s) reached; persisting as low_confidence", REPAIR_MAX_RETRIES)
        return "persist"
    return "repair"


# ── Build and compile graph ───────────────────────────────────────────────

def _build_graph():
    try:
        from langgraph.graph import StateGraph, END
    except ImportError:
        from langgraph.graph.state import StateGraph
        END = "__end__"
        try:
            from langgraph.prebuilt import END as LE
            END = LE
        except ImportError:
            pass

    graph = StateGraph(IngestionState)

    graph.add_node("pdf_parser", _node_pdf_parser)
    graph.add_node("table_extract", _node_table_extract)
    graph.add_node("page_fusion", _node_page_fusion)
    graph.add_node("section_chunker", _node_section_chunker)
    graph.add_node("vector_indexer", _node_vector_indexer)
    graph.add_node("clause_mapping", _node_clause_mapping)
    graph.add_node("schema_validation", _node_schema_validation)
    graph.add_node("ingestion_eval", _node_ingestion_eval)
    graph.add_node("llm_reviewer", _node_llm_reviewer)
    graph.add_node("persist", _node_persist)

    graph.set_entry_point("pdf_parser")
    graph.add_edge("pdf_parser", "table_extract")
    graph.add_edge("table_extract", "page_fusion")
    graph.add_edge("page_fusion", "section_chunker")
    graph.add_edge("section_chunker", "vector_indexer")
    graph.add_edge("vector_indexer", "clause_mapping")
    graph.add_edge("clause_mapping", "schema_validation")
    graph.add_edge("schema_validation", "ingestion_eval")
    graph.add_edge("ingestion_eval", "llm_reviewer")

    def route_after_review(state: IngestionState):
        dest = _should_repair(state)
        if dest == "repair":
            return "clause_mapping"  # re-run mapping then reviewer
        return "persist"

    graph.add_conditional_edges("llm_reviewer", route_after_review)
    graph.add_edge("persist", END)

    return graph.compile()


# Lazy-compiled graph
_compiled_graph = None


def _get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = _build_graph()
    return _compiled_graph


# ── Public API ────────────────────────────────────────────────────────────

def run_ingestion(pdf_path: str) -> IngestionResult:
    """
    Run the full ingestion DAG on a PDF. Returns IngestionResult with status and rules_count.

    If no test PDF is available or nodes fail, status may be "partial" or "failed".
    """
    path = Path(pdf_path)
    if not path.exists():
        return IngestionResult(status="failed", rules_count=0, message=f"PDF not found: {pdf_path}")

    initial: IngestionState = {
        "pdf_path": str(path.absolute()),
        "pages": [],
        "tables_per_page": [],
        "fused_pages": [],
        "section_chunks": [],
        "vector_index_info": {},
        "draft_rules": [],
        "validated_rules": [],
        "confidence": 0.0,
        "eval_metrics": {},
        "repair_count": 0,
    }

    try:
        graph = _get_graph()
        # LangGraph invoke: run until END
        final = graph.invoke(initial)
    except Exception as e:
        logger.exception("Ingestion DAG failed: %s", e)
        return IngestionResult(status="failed", rules_count=0, message=str(e))

    validated = final.get("validated_rules") or []
    status = final.get("status") or ("success" if len(validated) > 0 else "partial")
    if not validated and final.get("confidence", 0) < REPAIR_CONFIDENCE_THRESHOLD:
        status = "low_confidence"

    return IngestionResult(
        status=status,
        rules_count=len(validated),
        message=final.get("message", ""),
        eval_metrics=final.get("eval_metrics") or {},
    )
