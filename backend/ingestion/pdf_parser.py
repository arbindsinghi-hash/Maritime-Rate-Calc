"""
PDF Parser Node.

Extract per-page text and bounding boxes via PyMuPDF.
Returns List[PageExtract] for downstream table extract and clause mapping.
"""

from pathlib import Path
from typing import List

try:
    import fitz  # PyMuPDF

    # Suppress harmless "No common ancestor in structure tree" warnings
    # from malformed tagged-PDF metadata in the tariff PDF.
    fitz.TOOLS.mupdf_display_errors(False)
except ImportError:
    fitz = None

from backend.models.ingestion_models import PageExtract


def parse_pdf(pdf_path: str) -> List[PageExtract]:
    """
    Parse a PDF file and return one PageExtract per page with text and bbox list.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        List of PageExtract, one per page, each with non-empty text and bbox list.
    """
    if fitz is None:
        raise ImportError("PyMuPDF (fitz) is required for PDF parsing. Install with: pip install pymupdf")

    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    doc = fitz.open(pdf_path)
    pages: List[PageExtract] = []

    try:
        for page_num in range(len(doc)):
            page = doc[page_num]
            text = page.get_text()
            bbox_list: List[list] = []
            for block in page.get_text("dict")["blocks"]:
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        bbox = span.get("bbox")
                        if bbox and len(bbox) >= 4:
                            bbox_list.append([float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])])
            pages.append(
                PageExtract(
                    page_number=page_num + 1,
                    text=text or "",
                    bbox=bbox_list,
                )
            )
    finally:
        doc.close()

    return pages
