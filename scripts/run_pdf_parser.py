"""
Run pdf_parser on the full Port Tariff PDF and save structured output.

Usage:
    cd mrca-ai-tariff && python scripts/run_pdf_parser.py
"""

import json
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.ingestion.pdf_parser import parse_pdf

PDF_PATH = ROOT / "storage" / "pdfs" / "Port Tariff.pdf"
OUTPUT_DIR = ROOT / "output"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Parsing: {PDF_PATH}")
    print(f"File size: {PDF_PATH.stat().st_size / 1024:.1f} KB")
    print("-" * 60)

    pages = parse_pdf(str(PDF_PATH))

    print(f"Total pages extracted: {len(pages)}")
    print()

    # ── Summary per page ─────────────────────────────────────────────
    summary = []
    for p in pages:
        text_len = len(p.text)
        bbox_count = len(p.bbox)
        preview = p.text[:120].replace("\n", " ").strip()
        summary.append({
            "page": p.page_number,
            "text_chars": text_len,
            "bbox_count": bbox_count,
            "preview": preview,
        })
        print(f"  Page {p.page_number:3d}  |  {text_len:6d} chars  |  {bbox_count:5d} bboxes  |  {preview[:80]}...")

    print()

    # ── Save full output as JSON ─────────────────────────────────────
    full_output = []
    for p in pages:
        full_output.append({
            "page_number": p.page_number,
            "text": p.text,
            "bbox": p.bbox,
        })

    full_path = OUTPUT_DIR / "pdf_parser_full_output.json"
    with open(full_path, "w") as f:
        json.dump(full_output, f, indent=2, ensure_ascii=False)
    print(f"Full output saved: {full_path} ({full_path.stat().st_size / 1024:.1f} KB)")

    # ── Save summary as JSON ─────────────────────────────────────────
    summary_path = OUTPUT_DIR / "pdf_parser_summary.json"
    with open(summary_path, "w") as f:
        json.dump({
            "pdf_file": str(PDF_PATH.name),
            "total_pages": len(pages),
            "total_text_chars": sum(s["text_chars"] for s in summary),
            "total_bboxes": sum(s["bbox_count"] for s in summary),
            "pages": summary,
        }, f, indent=2, ensure_ascii=False)
    print(f"Summary saved: {summary_path}")

    # ── Save each page's text as individual .txt files ───────────────
    pages_dir = OUTPUT_DIR / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    for p in pages:
        page_file = pages_dir / f"page_{p.page_number:03d}.txt"
        with open(page_file, "w") as f:
            f.write(p.text)
    print(f"Individual page texts saved: {pages_dir}/ ({len(pages)} files)")

    # ── Stats ────────────────────────────────────────────────────────
    empty_pages = [s["page"] for s in summary if s["text_chars"] == 0]
    zero_bbox = [s["page"] for s in summary if s["bbox_count"] == 0]
    print()
    print("=" * 60)
    print(f"  Pages with empty text:     {len(empty_pages)} {empty_pages if empty_pages else ''}")
    print(f"  Pages with zero bboxes:    {len(zero_bbox)} {zero_bbox if zero_bbox else ''}")
    print(f"  Avg text chars/page:       {sum(s['text_chars'] for s in summary) / len(summary):.0f}")
    print(f"  Avg bboxes/page:           {sum(s['bbox_count'] for s in summary) / len(summary):.0f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
