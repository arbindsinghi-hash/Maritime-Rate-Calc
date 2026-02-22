"""
Vector Indexer Node.

Embed section chunks and store in FAISS with metadata.
Metadata is persisted alongside the FAISS index so that search results
can be mapped back to section_id, section_name, pages, etc.

Input:  list of section-chunk dicts (from section_chunker node).
Output: updated state with chunk_count and FAISS index path.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from backend.core.config import settings

logger = logging.getLogger(__name__)

METADATA_FILENAME = "tariff_chunks_metadata.json"


def index_section_chunks(
    section_chunks: List[dict],
    *,
    rebuild: bool = True,
) -> Dict[str, Any]:
    """Embed section chunks and add to FAISS index.

    Args:
        section_chunks: list of SectionChunk dicts from section_chunker.
        rebuild: if True, clear existing index before adding (fresh ingestion).

    Returns:
        dict with keys: chunk_count, index_path, metadata_path.
    """
    from backend.services.faiss_service import FAISSService

    if not section_chunks:
        logger.warning("No section chunks to index")
        return {"chunk_count": 0, "index_path": "", "metadata_path": ""}

    # Filter out preamble / empty chunks
    indexable = [c for c in section_chunks if c.get("text", "").strip() and c.get("section_id") != "0"]
    if not indexable:
        logger.warning("No indexable section chunks (all preamble or empty)")
        return {"chunk_count": 0, "index_path": "", "metadata_path": ""}

    texts = [c["text"] for c in indexable]
    metadata = [
        {
            "section_id": c.get("section_id", ""),
            "section_name": c.get("section_name", ""),
            "pages": c.get("pages", []),
            "has_tables": c.get("has_tables", False),
            "element_count": c.get("element_count", 0),
            "text_length": len(c.get("text", "")),
        }
        for c in indexable
    ]

    faiss_svc = FAISSService()

    if rebuild:
        # Fresh index for this ingestion run
        import faiss
        import numpy as np
        faiss_svc.index = faiss.IndexFlatL2(faiss_svc.dimension)
        faiss_svc._metadata = []

    try:
        faiss_svc.add_texts(texts, metadata=metadata)
        n = faiss_svc.index.ntotal
        logger.info("FAISS index now has %d vectors after adding %d section chunks", n, len(texts))
    except Exception as exc:
        logger.warning("FAISS embedding/indexing failed (endpoint may be unreachable): %s", exc)
        logger.info("Continuing without vector index — clause_mapping will use direct content")
        return {
            "chunk_count": len(indexable),
            "index_path": "",
            "metadata_path": "",
            "error": str(exc),
        }

    # Persist metadata alongside the FAISS index
    meta_path = os.path.join(settings.FAISS_INDEX_DIR, METADATA_FILENAME)
    os.makedirs(settings.FAISS_INDEX_DIR, exist_ok=True)
    with open(meta_path, "w") as f:
        json.dump(faiss_svc._metadata, f, indent=2, default=str)

    logger.info(
        "Section chunks indexed: %d chunks, FAISS ntotal=%d, metadata=%s",
        len(indexable), faiss_svc.index.ntotal, meta_path,
    )

    return {
        "chunk_count": len(indexable),
        "index_path": faiss_svc.index_path,
        "metadata_path": meta_path,
    }
