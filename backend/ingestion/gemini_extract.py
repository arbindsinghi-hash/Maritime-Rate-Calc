"""
Gemini Extract — send page images + OCR text to Gemini 2.5 Pro and return
structured elements (text, headers, tables) per page.

Pipeline:  PDF → PyMuPDF (image + OCR text) → Gemini 2.5 Pro → parsed elements

This replaces the old nemotron-parse table_extract module with a single,
simpler VL model call that handles *all* page content — not just tables.
"""

import base64
import re
import time
from typing import Any, Dict, List, Optional

try:
    import fitz  # PyMuPDF

    fitz.TOOLS.mupdf_display_errors(False)
except ImportError:
    fitz = None

from backend.core.config import settings
from backend.core.llm_clients import get_gemini_client
from backend.models.ingestion_models import PageExtract

# Extraction configuration
EXTRACT_MAX_TOKENS = 8192
EXTRACT_TEMPERATURE = 0.1
PAGE_RENDER_DPI = 150
MAX_RETRY_ATTEMPTS = 3


# ── Prompt ──────────────────────────────────────────────────────

EXTRACT_PROMPT = """\
You are a precise document OCR assistant.  You will be given:
1. A **page image** from a port tariff PDF.
2. The **raw OCR text** extracted from that same page (may have layout
   artefacts, missing structure, or OCR errors).

Use **both** inputs together — the image is the ground truth for layout,
tables, and visual structure; the OCR text helps with exact spelling,
numbers, and characters that may be hard to read from the image alone.

Your task:
1.  Produce a **complete, structured Markdown** rendition of the page
    that preserves every visible element in reading order.
2.  Format each element type as follows:
    - Section headers → ``## <number> <TITLE>`` (use ## for all heading levels)
    - Body paragraphs → plain text
    - Bullet / numbered lists → ``- item`` or ``1. item``
    - Tables → standard Markdown pipe tables with a header row and
      separator (``| --- |``).  Preserve **every row** and **every column**
      exactly as shown.  Do NOT merge rows.  Include all numeric values.
    - Footnotes / footers → plain text at the end

Important rules:
- Do NOT summarise or omit any content.
- Do NOT invent data that is not visible on the page.
- For tables, if a cell spans multiple rows, repeat the label in each row.
- Cross-check numbers and text between the image and the OCR text —
  prefer the image when they disagree, but use the OCR text to confirm
  exact values (decimals, currencies, units).
- Output **only** the Markdown — no commentary, no code fences.
"""


# ── Helpers ─────────────────────────────────────────────────────


def _page_to_image_base64(pdf_path: str, page_number: int, dpi: int = PAGE_RENDER_DPI) -> str:
    """Render a single PDF page to a base64-encoded PNG."""
    if fitz is None:
        raise ImportError("PyMuPDF required — pip install pymupdf")
    doc = fitz.open(pdf_path)
    try:
        page = doc[page_number - 1]
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        return base64.standard_b64encode(pix.tobytes("png")).decode("ascii")
    finally:
        doc.close()


def _parse_markdown_table(lines: List[str]) -> Dict[str, Any]:
    """Parse contiguous Markdown pipe-table lines into
    ``{"header": [...], "rows": [[...], ...]}``."""
    rows: List[List[str]] = []
    for line in lines:
        line = line.strip()
        if not line.startswith("|"):
            continue
        # Skip separator rows  | --- | --- | or | :--- | :---: |
        if re.match(r"^\|[\s\-:|]+\|$", line.replace(" ", "")):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        # Skip rows where all cells look like separators (e.g. ['---', '---'])
        if all(re.match(r"^[-:]+$", c) for c in cells if c):
            continue
        rows.append(cells)

    if not rows:
        return {"rows": []}
    return {"header": rows[0], "rows": rows[1:] if len(rows) > 1 else []}


def _classify_line(line: str) -> str:
    """Classify a single Markdown line by element type."""
    s = line.strip()
    if not s:
        return "Empty"
    if s.startswith("#"):
        return "Section-header"
    if s.startswith("|"):
        return "Table"
    if re.match(r"^[-*•]\s", s):
        return "List-item"
    if re.match(r"^\d+\.\s", s):
        return "List-item"
    return "Text"


def _parse_model_response(content: str) -> List[Dict[str, Any]]:
    """Parse Gemini's Markdown response into a list of element dicts.

    Returns ``[{"type": str, "text": str, "bbox": {}, ...}, ...]``.
    Table elements carry an extra ``parsed_table`` key.
    """
    # Strip code fences if present
    content = re.sub(r"^```(?:markdown)?\s*\n", "", content, flags=re.MULTILINE)
    content = re.sub(r"\n```\s*$", "", content, flags=re.MULTILINE)

    lines = content.split("\n")
    elements: List[Dict[str, Any]] = []
    i = 0

    while i < len(lines):
        kind = _classify_line(lines[i])

        if kind == "Empty":
            i += 1
            continue

        if kind == "Table":
            table_lines: List[str] = []
            while i < len(lines) and (
                _classify_line(lines[i]) == "Table"
                or re.match(r"^\s*\|[\s\-:]+\|\s*$", lines[i].replace(" ", ""))
            ):
                table_lines.append(lines[i])
                i += 1
            parsed = _parse_markdown_table(table_lines)
            elements.append({
                "type": "Table",
                "text": "",
                "bbox": {},
                "parsed_table": parsed,
            })
            continue

        text = lines[i].strip().lstrip("#").strip()
        elements.append({"type": kind, "text": text, "bbox": {}})
        i += 1

    return elements


# ── Main extraction ─────────────────────────────────────────────


def extract_page(
    pdf_path: str,
    page_number: int,
    ocr_text: str = "",
) -> Dict[str, Any]:
    """Extract structured content from a single PDF page via Gemini.

    Returns a dict compatible with the fusion pipeline::

        {
            "page": int,
            "tables": [{"header": [...], "rows": [[...]], "bbox": {}}, ...],
            "elements": [{"type": str, "text": str, "bbox": {}}, ...],
        }
    """
    b64 = _page_to_image_base64(pdf_path, page_number)

    content_parts: List[Dict[str, Any]] = [
        {"type": "text", "text": EXTRACT_PROMPT},
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}},
    ]
    if ocr_text.strip():
        content_parts.append({
            "type": "text",
            "text": f"\n--- RAW OCR TEXT (Page {page_number}) ---\n{ocr_text}\n--- END OCR TEXT ---",
        })

    client = get_gemini_client()
    response = client.chat.completions.create(
        model=settings.GEMINI_MODEL,
        messages=[{"role": "user", "content": content_parts}],
        max_tokens=EXTRACT_MAX_TOKENS,
        temperature=EXTRACT_TEMPERATURE,
    )
    raw_md = response.choices[0].message.content or ""
    elements = _parse_model_response(raw_md)

    # Split elements into tables list + full elements list
    tables: List[Dict[str, Any]] = []
    clean_elements: List[Dict[str, Any]] = []
    for el in elements:
        if el["type"] == "Table":
            parsed = el.get("parsed_table", {"rows": []})
            tables.append({
                "header": parsed.get("header", []),
                "rows": parsed.get("rows", []),
                "bbox": {},
            })
        clean_elements.append({
            "type": el["type"],
            "text": el["text"],
            "bbox": el.get("bbox", {}),
        })

    return {"page": page_number, "tables": tables, "elements": clean_elements}


def extract_all_pages(
    pdf_path: str,
    pages: Optional[List[PageExtract]] = None,
    page_numbers: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Extract all pages from a PDF via Gemini 2.5 Pro.

    Args:
        pdf_path: Path to the PDF.
        pages: Pre-parsed PageExtract list (for OCR text). Optional.
        page_numbers: Subset of 1-based page numbers. None → all pages.

    Returns:
        List of per-page dicts (same format as ``extract_page``).
    """
    if fitz is None:
        raise ImportError("PyMuPDF required — pip install pymupdf")

    doc = fitz.open(pdf_path)
    total = len(doc)
    doc.close()

    if page_numbers is None:
        page_numbers = list(range(1, total + 1))

    # Index OCR text by page number
    ocr_index: Dict[int, str] = {}
    if pages:
        for p in pages:
            ocr_index[p.page_number] = p.text or ""

    results: List[Dict[str, Any]] = []
    for pnum in page_numbers:
        if pnum < 1 or pnum > total:
            continue
        t0 = time.time()
        print(f"  Page {pnum}/{total} … ", end="", flush=True)

        last_exc: Exception = RuntimeError("unknown")
        result = None
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                result = extract_page(pdf_path, pnum, ocr_index.get(pnum, ""))
                break                        # success
            except Exception as exc:
                last_exc = exc
                elapsed = time.time() - t0
                if attempt < MAX_RETRY_ATTEMPTS:
                    wait = attempt * 10      # 10 s, 20 s
                    print(f"\n    attempt {attempt} failed ({elapsed:.0f}s): {exc!r} — retrying in {wait}s …", end="", flush=True)
                    time.sleep(wait)

        elapsed = time.time() - t0
        if result is not None:
            n_t = len(result["tables"])
            n_e = len(result["elements"])
            print(f"{n_t} table(s), {n_e} element(s)  ({elapsed:.1f}s)")
            results.append(result)
        else:
            print(f"FAILED after {MAX_RETRY_ATTEMPTS} attempts ({elapsed:.1f}s): {last_exc}")
            results.append({"page": pnum, "tables": [], "elements": [], "error": str(last_exc)})

    return results
