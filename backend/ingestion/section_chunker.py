"""
Section Chunker Node.

Split fused pages into section-wise chunks.  Each chunk corresponds to a
distinct tariff section (e.g. "1.1 Light Dues", "2.1 VTS Charges") and
contains the full Markdown text for that section across however many pages
it spans.

Input:  list of fused-page dicts (from page_fusion node).
Output: list of SectionChunk dicts, each with:
          section_id   — normalised section identifier (e.g. "1.1")
          section_name — human-readable name
          text         — concatenated Markdown content
          pages        — list of page numbers the section spans
          has_tables   — whether the chunk contains any tables
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Pattern to detect section headers like "SECTION 1", "1.1 LIGHT DUES", "2.1.1 VTS CHARGES"
_SECTION_RE = re.compile(
    r"^(?:SECTION\s+)?(\d+(?:\.\d+)*)\s+(.*)",
    re.IGNORECASE,
)


@dataclass
class SectionChunk:
    """One tariff-section chunk ready for embedding and clause mapping."""

    section_id: str          # e.g. "1.1" or "2.1.1"
    section_name: str        # e.g. "Light Dues on Vessels"
    text: str                # full Markdown content
    pages: List[int] = field(default_factory=list)
    has_tables: bool = False
    element_count: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _parse_section_id(header: str) -> tuple[str, str] | None:
    """Extract (section_id, section_name) from a header string.

    Examples:
        "SECTION 1"          → ("1", "")
        "1.1 LIGHT DUES"    → ("1.1", "LIGHT DUES")
        "2.1.1 VTS CHARGES" → ("2.1.1", "VTS CHARGES")
        "Definitions"       → None
    """
    header = header.strip()
    # "SECTION N" alone
    m = re.match(r"^SECTION\s+(\d+)\s*$", header, re.IGNORECASE)
    if m:
        return (m.group(1), "")
    # "N.N.N TITLE" or "SECTION N.N TITLE"
    m = _SECTION_RE.match(header)
    if m:
        return (m.group(1), m.group(2).strip())
    return None


def chunk_fused_pages(fused_pages: List[dict]) -> List[dict]:
    """Split fused pages into section-wise chunks.

    Algorithm:
    1. Walk every element on every page in order.
    2. When a header element matches a section pattern, start a new chunk.
    3. All subsequent elements go into the current chunk until the next
       section header is found.
    4. Non-section content (cover, TOC, definitions) before the first
       numbered section is collected into a "preamble" chunk.

    Args:
        fused_pages: list of fused-page dicts (from FusedPage.to_dict()).

    Returns:
        list of SectionChunk dicts.
    """
    from backend.ingestion.page_fusion import FusedElement, FusedPage

    chunks: List[SectionChunk] = []
    current: SectionChunk | None = None

    for fp_dict in fused_pages:
        page_number = fp_dict.get("page_number", 0)
        elements = fp_dict.get("elements", [])

        # Check the page-level section_header first
        page_hdr = fp_dict.get("section_header", "")
        if page_hdr:
            parsed = _parse_section_id(page_hdr)
            if parsed:
                sec_id, sec_name = parsed
                # Only start new chunk if it's a different section
                if current is None or current.section_id != sec_id:
                    if current is not None:
                        chunks.append(current)
                    current = SectionChunk(
                        section_id=sec_id,
                        section_name=sec_name,
                        text="",
                        pages=[page_number],
                    )

        for elem_dict in elements:
            kind = elem_dict.get("kind", "text")
            text = elem_dict.get("text", "")
            table = elem_dict.get("table")

            # Check if this element is a section header
            if kind == "header" and text.strip():
                parsed = _parse_section_id(text)
                if parsed:
                    sec_id, sec_name = parsed
                    # Start a new chunk for each new section
                    if current is None or current.section_id != sec_id:
                        if current is not None:
                            chunks.append(current)
                        current = SectionChunk(
                            section_id=sec_id,
                            section_name=sec_name,
                            text="",
                            pages=[page_number],
                        )
                    elif sec_name and not current.section_name:
                        # Fill in the name if the first header had only "SECTION N"
                        current.section_name = sec_name

            # If no current chunk yet, start a preamble chunk
            if current is None:
                current = SectionChunk(
                    section_id="0",
                    section_name="Preamble",
                    text="",
                    pages=[page_number],
                )

            # Append content to current chunk
            if kind == "table" and table:
                current.has_tables = True
                current.text += _table_to_text(table) + "\n\n"
            elif text.strip():
                if kind == "header":
                    current.text += f"### {text.strip()}\n\n"
                else:
                    current.text += text.strip() + "\n\n"

            current.element_count += 1

            # Track page numbers
            if page_number not in current.pages:
                current.pages.append(page_number)

    # Don't forget the last chunk
    if current is not None:
        chunks.append(current)

    # Filter out preamble and empty chunks, but keep numbered sections
    result = []
    for chunk in chunks:
        if not chunk.text.strip():
            continue
        result.append(chunk)

    logger.info(
        "Section chunker produced %d chunks from %d fused pages: %s",
        len(result),
        len(fused_pages),
        [(c.section_id, c.section_name, len(c.text)) for c in result],
    )

    return [c.to_dict() for c in result]


def _table_to_text(table: dict) -> str:
    """Render a table dict as a pipe-delimited Markdown table."""
    header = table.get("header", [])
    rows = table.get("rows", [])
    if not rows and not header:
        return ""

    ncols = max(len(header) if header else 0, max((len(r) for r in rows), default=0))

    def pad(cells: list) -> list:
        return list(cells) + [""] * (ncols - len(cells))

    lines = []
    if header:
        lines.append("| " + " | ".join(str(c) for c in pad(header)) + " |")
        lines.append("| " + " | ".join("---" for _ in range(ncols)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in pad(row)) + " |")
    return "\n".join(lines)
