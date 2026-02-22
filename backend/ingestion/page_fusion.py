"""
Page Fusion — merge PDF-parser text with Gemini-extracted elements.

PDF Parser provides:  raw text per page, bounding boxes per text span.
Gemini Extract provides:  structured elements (headers, text, tables, lists)
  in reading order — already correctly ordered by the VL model.

This module produces a unified ``FusedPage`` per page combining both sources.
When Gemini elements are available they are the primary source.  Any PDF-parser
lines missed by Gemini are recovered as supplementary elements.

Output: list of ``FusedPage`` consumed by clause_mapping and downstream.
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from backend.models.ingestion_models import PageExtract

logger = logging.getLogger(__name__)


# ── Text helpers ────────────────────────────────────────────────


def _strip_md_heading(text: str) -> str:
    """Strip leading markdown heading markers and bold wrappers."""
    s = text.strip()
    s = re.sub(r"^#+\s*", "", s)
    if s.startswith("**") and s.endswith("**"):
        s = s[2:-2].strip()
    return s


def _norm(text: str) -> str:
    """Normalise text for fuzzy substring matching."""
    s = text.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("**", "")
    s = s.replace("\u201c", '"').replace("\u201d", '"')
    s = s.replace("\u2018", "'").replace("\u2019", "'")
    s = s.replace("\u2014", "-").replace("\u2013", "-")
    s = s.replace("\u2026", "...")
    s = s.replace("\u2022", "")
    s = re.sub(r"\\?\(\\?bullet\\?\)", "", s)
    s = re.sub(r"</?[a-z][a-z0-9]*[^>]*>", "", s)
    s = re.sub(r"\.{2,}", " ... ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── Data structures ─────────────────────────────────────────────


@dataclass
class FusedElement:
    """One logical element on a page — prose text or a structured table."""

    kind: str  # "text" | "table" | "header" | "list-item" | "footer"
    y_position: float  # vertical position (0..1) for ordering
    x_position: float = 0.0
    text: str = ""
    table: Optional[Dict[str, Any]] = None  # {"header": [...], "rows": [[...]]}
    source: str = ""  # "gemini" | "pdf_parser"
    confidence: float = 1.0
    needs_reconfirmation: bool = False


@dataclass
class FusedPage:
    """Unified representation of one PDF page."""

    page_number: int
    elements: List[FusedElement] = field(default_factory=list)
    section_header: str = ""
    has_tables: bool = False
    table_count: int = 0
    reconfirmation_manifest: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_markdown(self) -> str:
        """Render the fused page as clean Markdown."""
        parts: List[str] = []
        parts.append(f"## Page {self.page_number}")
        if self.section_header:
            clean_hdr = _strip_md_heading(self.section_header)
            parts.append(f"### {clean_hdr}")

        seen_headers: set = set()
        if self.section_header:
            seen_headers.add(_strip_md_heading(self.section_header).lower())

        for elem in self.elements:
            if elem.kind == "header":
                clean = _strip_md_heading(elem.text)
                if clean.lower() in seen_headers:
                    continue
                seen_headers.add(clean.lower())
                parts.append(f"### {clean}")
            elif elem.kind == "list-item":
                parts.append(f"- {elem.text}")
            elif elem.kind == "footer":
                continue
            elif elem.kind == "table" and elem.table:
                parts.append(_table_to_markdown(elem.table))
            elif elem.kind == "text":
                if elem.text.strip():
                    parts.append(elem.text.strip())

        return "\n\n".join(parts)


# ── Table rendering ─────────────────────────────────────────────


def _table_to_markdown(table: Dict[str, Any]) -> str:
    """Convert a parsed table dict to a Markdown pipe-table."""
    header = table.get("header", [])
    rows = table.get("rows", [])

    if not rows and not header:
        return ""

    ncols = max(len(header) if header else 0, max((len(r) for r in rows), default=0))
    if ncols == 0:
        return ""

    lines: List[str] = []
    if header:
        h = header + [""] * (ncols - len(header))
        lines.append("| " + " | ".join(h) + " |")
        lines.append("| " + " | ".join(["---"] * ncols) + " |")
    else:
        h = rows[0] + [""] * (ncols - len(rows[0]))
        lines.append("| " + " | ".join(h) + " |")
        lines.append("| " + " | ".join(["---"] * ncols) + " |")
        rows = rows[1:]

    for row in rows:
        r = row + [""] * (ncols - len(row))
        lines.append("| " + " | ".join(r) + " |")

    return "\n".join(lines)


# ── Core fusion ─────────────────────────────────────────────────


def _elements_from_gemini(
    all_elements: List[Dict[str, Any]],
    tables: List[Dict[str, Any]],
    fused: FusedPage,
    page_text: str = "",
) -> List[FusedElement]:
    """Convert Gemini-extracted elements into FusedElements.

    Gemini elements arrive in reading order.  We assign sequential
    y-positions to preserve that order.
    """
    total = max(len(all_elements), 1)
    table_idx = 0
    elements: List[FusedElement] = []

    for i, elem in enumerate(all_elements):
        etype = elem.get("type", "Text")
        text = elem.get("text", "")
        y_pos = i / total

        if etype == "Table":
            table_data = tables[table_idx] if table_idx < len(tables) else None
            table_idx += 1

            conf, missing = (
                _score_table_confidence(table_data, page_text)
                if table_data and page_text
                else (1.0, [])
            )
            needs_reconf = conf < _TABLE_CONFIDENCE_THRESHOLD

            if needs_reconf and table_data:
                table_data["_missing_values"] = missing

            elements.append(FusedElement(
                kind="table", y_position=y_pos, table=table_data,
                source="gemini", confidence=conf, needs_reconfirmation=needs_reconf,
            ))

        elif etype == "Section-header":
            elements.append(FusedElement(kind="header", y_position=y_pos, text=text, source="gemini"))
            if not fused.section_header:
                fused.section_header = text

        elif etype == "List-item":
            elements.append(FusedElement(kind="list-item", y_position=y_pos, text=text, source="gemini"))

        elif etype == "Page-footer":
            elements.append(FusedElement(kind="footer", y_position=y_pos, text=text, source="gemini"))

        else:
            elements.append(FusedElement(kind="text", y_position=y_pos, text=text, source="gemini"))

    return elements


def _elements_from_pdf_parser(page: PageExtract) -> List[FusedElement]:
    """Fallback: build elements from PDF-parser text when no Gemini data."""
    lines = page.text.split("\n")
    total = max(len(lines), 1)
    elements: List[FusedElement] = []

    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        y_pos = i / total

        if re.match(r"^[1-9]\d*\.\d+(\.\d+)?\s+[A-Za-z]", s) and len(s) < 120:
            elements.append(FusedElement(kind="header", y_position=y_pos, text=s, source="pdf_parser"))
        elif s.startswith("\u2022") or s.startswith("-"):
            elements.append(FusedElement(
                kind="list-item", y_position=y_pos,
                text=s.lstrip("\u2022- ").strip(), source="pdf_parser",
            ))
        elif re.match(r"^Tariff Book\s", s, re.IGNORECASE):
            elements.append(FusedElement(kind="footer", y_position=y_pos, text=s, source="pdf_parser"))
        else:
            elements.append(FusedElement(kind="text", y_position=y_pos, text=s, source="pdf_parser"))

    return elements


def _recover_orphan_lines(
    page: PageExtract,
    gemini_elements: List[FusedElement],
) -> List[FusedElement]:
    """Find PDF-parser text lines not covered by any Gemini element."""
    if not page.text.strip():
        return []

    gemini_texts = [_norm(e.text) for e in gemini_elements if e.text]

    lines = page.text.split("\n")
    total = max(len(lines), 1)
    y_max = max((e.y_position for e in gemini_elements), default=1.0)
    orphans: List[FusedElement] = []

    for i, line in enumerate(lines):
        s = line.strip()
        if not s or len(s) < 3:
            continue
        if re.match(r"^Tariff Book\s", s, re.IGNORECASE):
            continue
        if re.match(r"^[\d\s,./%\u2014\-]+$", s):
            continue

        norm_line = _norm(s)
        if len(norm_line) < 3:
            continue

        covered = any(norm_line in gt for gt in gemini_texts)
        if not covered:
            first_words = " ".join(norm_line.split()[:4])
            if len(first_words) >= 6 and first_words != norm_line:
                covered = any(first_words in gt for gt in gemini_texts)
        if covered:
            continue

        y_pos = (i / total) * y_max
        orphans.append(FusedElement(kind="text", y_position=y_pos, text=s, source="pdf_parser"))

    return orphans


# ── Confidence scoring ──────────────────────────────────────────

_TABLE_CONFIDENCE_THRESHOLD = 0.85


def _extract_numeric_values(text: str) -> set:
    """Extract distinct numeric tokens from text."""
    pattern = r"\b(\d[\d ,]*(?:\.\d+)?)\b"
    values = set()
    for m in re.finditer(pattern, text):
        canon = m.group(1).replace(" ", "").replace(",", "")
        if "." not in canon and len(canon) < 2:
            continue
        values.add(canon)
    return values


def _score_table_confidence(
    table: Dict[str, Any],
    page_text: str,
) -> tuple:
    """Score a table's completeness against PDF-parser text.

    Returns (confidence, missing_values).
    """
    rows = table.get("rows", [])
    header = table.get("header", [])
    if not rows:
        return 1.0, []

    table_text = " ".join(cell for row in rows for cell in row)
    if header:
        table_text += " " + " ".join(header)
    table_vals = {v for v in _extract_numeric_values(table_text) if "." in v}

    if not table_vals:
        return 1.0, []

    pdf_vals = {v for v in _extract_numeric_values(page_text) if "." in v}
    if len(pdf_vals) < 2:
        return 1.0, []

    missing = pdf_vals - table_vals
    matched = pdf_vals - missing
    confidence = len(matched) / len(pdf_vals) if pdf_vals else 1.0

    return round(confidence, 3), sorted(missing)


# ── Single-page fusion ─────────────────────────────────────────


def fuse_page(
    page: PageExtract,
    extract_result: Optional[dict] = None,
) -> FusedPage:
    """Fuse one page's PDF-parser output with its Gemini extraction."""
    fused = FusedPage(page_number=page.page_number)

    tables: List[Dict[str, Any]] = []
    all_elements: List[Dict[str, Any]] = []

    if extract_result:
        tables = extract_result.get("tables", [])
        all_elements = extract_result.get("elements", [])

    fused.has_tables = len(tables) > 0
    fused.table_count = len(tables)

    elements: List[FusedElement] = []

    if all_elements:
        elements = _elements_from_gemini(all_elements, tables, fused, page.text)
        orphans = _recover_orphan_lines(page, elements)
        if orphans:
            elements.extend(orphans)
    else:
        elements = _elements_from_pdf_parser(page)

    elements.sort(key=lambda e: e.y_position)

    if not fused.section_header:
        for elem in elements:
            if elem.kind == "header":
                fused.section_header = elem.text
                break

    for idx, elem in enumerate(elements):
        if elem.kind == "table" and elem.needs_reconfirmation:
            fused.reconfirmation_manifest.append({
                "page_number": page.page_number,
                "element_index": idx,
                "confidence": elem.confidence,
                "reason": "numeric_value_mismatch",
                "missing_values": elem.table.get("_missing_values", []) if elem.table else [],
            })

    fused.elements = elements
    return fused


# ── Batch fusion ────────────────────────────────────────────────


def fuse_all_pages(
    pages: List[PageExtract],
    extracts_per_page: List[dict],
    **_kwargs,
) -> List[FusedPage]:
    """Fuse all pages.

    Args:
        pages: Output of ``parse_pdf()`` — one ``PageExtract`` per page.
        extracts_per_page: Output of ``extract_all_pages()`` — one dict per page
            with ``"page"``, ``"tables"``, and ``"elements"`` keys.

    Returns:
        List of ``FusedPage`` in page order.
    """
    extract_index: Dict[int, dict] = {}
    for e in extracts_per_page:
        pnum = e.get("page")
        if pnum is not None:
            extract_index[pnum] = e

    fused: List[FusedPage] = []
    for page in pages:
        fused.append(fuse_page(page, extract_index.get(page.page_number)))

    return fused
