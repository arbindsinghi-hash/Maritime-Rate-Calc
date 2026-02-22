#!/usr/bin/env python3
"""
Run the full extraction pipeline:  PDF → PyMuPDF → Gemini 2.5 Pro → Fusion → Output.

Features:
  - Per-page checkpoint: results are saved after every page so a crash/kill
    never throws away completed work.  Re-running resumes from where it stopped.
  - --pages flag for single-page or subset runs.
  - 120 s per-page timeout + 3-attempt retry (see gemini_extract.py).

Usage:
    cd mrca-ai-tariff
    source .venv/bin/activate
    python scripts/run_extract.py                  # full run (resumes if partial)
    python scripts/run_extract.py --pages 9 14     # subset only
    python scripts/run_extract.py --fresh          # ignore all caches, start over
"""

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from backend.core.config import settings  # noqa: E402
from backend.ingestion.pdf_parser import parse_pdf  # noqa: E402
from backend.ingestion.gemini_extract import extract_page  # noqa: E402
from backend.ingestion.page_fusion import fuse_all_pages  # noqa: E402

PDF_PATH = ROOT / "storage" / "pdfs" / "Port Tariff.pdf"
OUTPUT_DIR = ROOT / "output"
CHECKPOINT_PATH = OUTPUT_DIR / "gemini_extract_checkpoint.json"
GEMINI_CACHE_PATH = OUTPUT_DIR / "gemini_extract.json"


def _load_checkpoint() -> dict[int, dict]:
    """Load per-page checkpoint dict keyed by page number."""
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH) as f:
            raw = json.load(f)
        return {int(k): v for k, v in raw.items()}
    return {}


def _save_checkpoint(checkpoint: dict[int, dict]) -> None:
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({str(k): v for k, v in checkpoint.items()}, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Gemini extraction pipeline")
    parser.add_argument(
        "--pages", type=int, nargs="+", metavar="N",
        help="Only process these 1-based page numbers (e.g. --pages 9 14)"
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Ignore all caches and checkpoints — start from scratch"
    )
    args = parser.parse_args()
    page_filter: list[int] | None = args.pages

    # --- Pre-flight ---
    if not settings.GEMINI_API_KEY:
        print("ERROR: GEMINI_API_KEY not set in .env")
        sys.exit(1)
    if not PDF_PATH.exists():
        print(f"ERROR: PDF not found at {PDF_PATH}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.fresh:
        for p in [CHECKPOINT_PATH, GEMINI_CACHE_PATH,
                  OUTPUT_DIR / "fused_pages_markdown.md",
                  OUTPUT_DIR / "fused_pages_full.json",
                  OUTPUT_DIR / "reconfirmation_manifest.json"]:
            p.unlink(missing_ok=True)
        print("✓ Cleared all caches")

    # ── Step 1: PDF Parser (PyMuPDF) ────────────────────────────
    pdf_cache = OUTPUT_DIR / "pdf_parser_full_output.json"
    if pdf_cache.exists():
        print(f"✓ Using cached PDF parser output: {pdf_cache.name}")
        with open(pdf_cache) as f:
            pdf_data = json.load(f)
        from backend.models.ingestion_models import PageExtract
        pages = [
            PageExtract(page_number=p["page_number"], text=p["text"], bbox=p.get("bbox", []))
            for p in pdf_data
        ]
    else:
        print(f"Parsing PDF: {PDF_PATH.name}")
        t0 = time.time()
        pages = parse_pdf(str(PDF_PATH))
        print(f"  ✓ {len(pages)} pages parsed in {time.time() - t0:.1f}s")
        with open(pdf_cache, "w") as f:
            json.dump([{"page_number": p.page_number, "text": p.text, "bbox": p.bbox}
                       for p in pages], f, indent=2, ensure_ascii=False)
        print(f"  → {pdf_cache.name}")

    total_pages = len(pages)
    ocr_index = {p.page_number: p.text or "" for p in pages}

    # ── Step 2: Gemini Extract (with per-page checkpoint) ───────
    if GEMINI_CACHE_PATH.exists() and not page_filter:
        print(f"✓ Using cached Gemini extraction: {GEMINI_CACHE_PATH.name}")
        with open(GEMINI_CACHE_PATH) as f:
            extracts = json.load(f)
    else:
        # Load checkpoint (pages already done)
        checkpoint = _load_checkpoint()

        # Determine which pages still need processing
        target_pages = page_filter if page_filter else list(range(1, total_pages + 1))
        remaining = [p for p in target_pages if p not in checkpoint]
        done_count = len(target_pages) - len(remaining)

        print(f"\n{'─' * 60}")
        print(f"  Extracting with Gemini ({settings.GEMINI_MODEL})")
        if page_filter:
            print(f"  Pages requested: {page_filter}")
        print(f"  PDF: {PDF_PATH.name}  |  {total_pages} pages total")
        if done_count:
            print(f"  Resuming: {done_count} page(s) already in checkpoint, "
                  f"{len(remaining)} remaining")
        print(f"{'─' * 60}")

        t0 = time.time()
        for pnum in remaining:
            if pnum < 1 or pnum > total_pages:
                continue
            page_t0 = time.time()
            print(f"  Page {pnum}/{total_pages} … ", end="", flush=True)

            last_exc: Exception = RuntimeError("unknown")
            result = None
            for attempt in range(1, 4):
                try:
                    result = extract_page(str(PDF_PATH), pnum, ocr_index.get(pnum, ""))
                    break
                except Exception as exc:
                    last_exc = exc
                    elapsed = time.time() - page_t0
                    if attempt < 3:
                        wait = attempt * 10
                        print(f"\n    attempt {attempt} failed ({elapsed:.0f}s): "
                              f"{type(exc).__name__} — retrying in {wait}s …",
                              end="", flush=True)
                        time.sleep(wait)

            elapsed = time.time() - page_t0
            if result is not None:
                n_t = len(result["tables"])
                n_e = len(result["elements"])
                print(f"{n_t} table(s), {n_e} element(s)  ({elapsed:.1f}s)")
                checkpoint[pnum] = result
                _save_checkpoint(checkpoint)   # ← checkpoint after every page
            else:
                print(f"FAILED after 3 attempts ({elapsed:.1f}s): {last_exc!r}")
                checkpoint[pnum] = {"page": pnum, "tables": [], "elements": [],
                                    "error": str(last_exc)}
                _save_checkpoint(checkpoint)

        total_elapsed = time.time() - t0
        print(f"\n  ✓ Extraction complete in {total_elapsed:.1f}s")

        # Assemble ordered extracts from checkpoint
        all_pages_done = page_filter if page_filter else list(range(1, total_pages + 1))
        extracts = [checkpoint[p] for p in all_pages_done if p in checkpoint]

        # Only write the final cache if we processed all pages (not a subset)
        if not page_filter:
            with open(GEMINI_CACHE_PATH, "w") as f:
                json.dump(extracts, f, indent=2)
            print(f"  → {GEMINI_CACHE_PATH.name}")
            CHECKPOINT_PATH.unlink(missing_ok=True)   # checkpoint no longer needed

    # ── Step 3: Fusion ──────────────────────────────────────────
    pages_to_fuse = [p for p in pages if page_filter is None or p.page_number in page_filter]
    print(f"\nFusing {len(pages_to_fuse)} page(s) …")
    fused_pages = fuse_all_pages(pages_to_fuse, extracts)

    # ── Write outputs ────────────────────────────────────────────
    md_path = OUTPUT_DIR / "fused_pages_markdown.md"
    with open(md_path, "w") as f:
        for fp in fused_pages:
            f.write(fp.to_markdown())
            f.write("\n\n---\n\n")

    json_path = OUTPUT_DIR / "fused_pages_full.json"
    with open(json_path, "w") as f:
        json.dump([fp.to_dict() for fp in fused_pages], f, indent=2)

    manifest: list[dict] = []
    for fp in fused_pages:
        manifest.extend(fp.reconfirmation_manifest)

    manifest_path = OUTPUT_DIR / "reconfirmation_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # ── Summary ──────────────────────────────────────────────────
    total_tables = sum(fp.table_count for fp in fused_pages)
    pages_w_tables = sum(1 for fp in fused_pages if fp.has_tables)
    total_elements = sum(len(fp.elements) for fp in fused_pages)

    print(f"\n{'═' * 60}")
    print(f"  ✓ {len(fused_pages)} pages  |  {total_tables} tables on "
          f"{pages_w_tables} pages  |  {total_elements} elements")
    print(f"{'═' * 60}")
    print(f"  → {md_path.name}")
    print(f"  → {json_path.name}")

    if manifest:
        print(f"\n  ⚠  {len(manifest)} table(s) flagged for reconfirmation:")
        for entry in manifest:
            print(f"     Page {entry['page_number']}: confidence={entry['confidence']:.1%}")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()

