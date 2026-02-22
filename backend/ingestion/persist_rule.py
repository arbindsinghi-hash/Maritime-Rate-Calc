"""
Persist Rule Node.

Write validated YAML to YAML_DIR, append citation records to JSONL audit store, index clauses in FAISS.
"""

import logging
from pathlib import Path
from typing import List

import yaml

from backend.core.config import settings
from backend.core.audit_store import audit_store
from backend.services.faiss_service import FAISSService

logger = logging.getLogger(__name__)


def persist_rules(
    validated_rules: List[dict],
    output_filename: str = "tariff_rules_ingested.yaml",
    faiss_service: FAISSService | None = None,
) -> tuple[int, int]:
    """
    Write validated rules to YAML, append citations to JSONL audit store, index clauses in FAISS.

    Args:
        validated_rules: List of dicts with charge_name, rate, basis, formula, citation.
        output_filename: Filename under YAML_DIR.
        faiss_service: FAISS service instance; if None, a new one is used.

    Returns:
        (files_written: 1 if YAML written else 0, citation_records_added: count).
    """
    yaml_dir = Path(settings.YAML_DIR)
    yaml_dir.mkdir(parents=True, exist_ok=True)
    out_path = yaml_dir / output_filename

    # Build YAML structure compatible with TariffRuleset (minimal for ingestion output)
    sections = []
    for r in validated_rules:
        cite = r.get("citation") or {}
        sections.append({
            "id": (r.get("charge_name") or "").lower().replace(" ", "_"),
            "name": r.get("charge_name", ""),
            "description": "",
            "citation": {"page": cite.get("page", 0), "section": cite.get("section", "")},
            "calculation": {
                "type": "per_unit",
                "basis": r.get("basis", ""),
                "rate": r.get("rate"),
                "rate_per_gt": r.get("rate") if isinstance(r.get("rate"), (int, float)) else None,
                "formula_note": r.get("formula", ""),
            },
        })

    data = {
        "metadata": {
            "schema_version": "1.0",
            "tariff_edition": "Ingested",
            "effective_from": "2024-01-01",
            "effective_to": "2025-12-31",
            "currency": "ZAR",
            "issuer": {"name": "Ingestion", "jurisdiction": "", "legal_basis": ""},
        },
        "definitions": {},
        "sections": sections,
    }

    with open(out_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    # Append citation records to JSONL audit store
    citation_records = 0
    try:
        for r in validated_rules:
            cite = r.get("citation") or {}
            audit_store.append(
                vessel_name=r.get("charge_name", "ingestion"),
                imo_number=None,
                input_data={"charge_name": r.get("charge_name", ""), "source": "ingestion"},
                output_data={
                    "page": cite.get("page", 0),
                    "section": cite.get("section", ""),
                    "bbox": cite.get("bounding_box") or cite.get("bbox"),
                },
                tariff_version="ingested",
            )
            citation_records += 1
    except Exception as exc:
        logger.error(
            "Failed to append citation records to audit store after %d records: %s",
            citation_records, exc,
        )

    # FAISS indexing (best-effort — may fail if embedding endpoint is unreachable)
    try:
        faiss = faiss_service or FAISSService()
        texts = [f"{r.get('charge_name', '')}: {r.get('formula', '')} ({r.get('basis', '')})" for r in validated_rules]
        if texts:
            meta = [{"charge_name": r.get("charge_name", ""), "page": (r.get("citation") or {}).get("page")} for r in validated_rules]
            faiss.add_texts(texts, metadata=meta)
    except Exception as exc:
        logger.warning("FAISS indexing skipped (embedding endpoint unavailable): %s", exc)

    return 1, citation_records
