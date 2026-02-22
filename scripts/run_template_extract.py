"""
run_template_extract.py — Feed extracted markdown sections through the JSON template.

Usage:
    # Extract all sections from the fused markdown
    python scripts/run_template_extract.py

    # Extract a specific section by page
    python scripts/run_template_extract.py --pages 7,8,9

    # Use the golden YAML to evaluate accuracy
    python scripts/run_template_extract.py --eval

    # Dry-run: show prompts without calling LLM
    python scripts/run_template_extract.py --dry-run

Pipeline:
    fused_pages_markdown.md
    → split into sections (by header pattern)
    → for each section: build prompt with JSON template
    → call Gemini / LLM to fill template
    → validate JSON against TariffSection Pydantic model
    → save validated sections to output/sections/<section_id>.json
    → optionally assemble into YAML
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.ingestion.section_template import (
    SYSTEM_PROMPT,
    build_section_extraction_prompt,
    build_metadata_extraction_prompt,
    build_definitions_extraction_prompt,
)
from backend.ingestion.template_to_yaml import (
    validate_section_json,
    assemble_ruleset,
    save_ruleset_yaml,
    clean_template_json,
)

# Output directories
SECTIONS_DIR = PROJECT_ROOT / "output" / "sections"
FUSED_MD = PROJECT_ROOT / "output" / "fused_pages_markdown.md"


# ─────────────────────────────────────────────────────────────────────────────
# Section splitter: split the fused markdown into individual sections
# ─────────────────────────────────────────────────────────────────────────────

# Pattern: "## Section X.Y" or "### X.Y.Z" or "## Page N" boundary markers
SECTION_HEADER_RE = re.compile(
    r'^#{1,3}\s+(?:Section\s+)?(\d+(?:\.\d+)*)\s*[-–—:.]?\s*(.*)',
    re.MULTILINE
)

PAGE_MARKER_RE = re.compile(r'^---\s*Page\s+(\d+)\s*---', re.MULTILINE)


def split_into_sections(markdown: str) -> List[Dict[str, Any]]:
    """
    Split fused markdown into sections based on header patterns.
    Returns list of {page: int, section: str, title: str, text: str}.
    """
    sections: List[Dict[str, Any]] = []

    # Track current page
    current_page = 1
    lines = markdown.split("\n")
    current_section: Optional[Dict[str, Any]] = None
    current_lines: List[str] = []

    for line in lines:
        # Check for page markers
        page_match = PAGE_MARKER_RE.match(line)
        if page_match:
            current_page = int(page_match.group(1))
            continue

        # Check for section headers
        header_match = SECTION_HEADER_RE.match(line)
        if header_match:
            # Save previous section
            if current_section and current_lines:
                current_section["text"] = "\n".join(current_lines).strip()
                if len(current_section["text"]) > 50:  # Skip trivial sections
                    sections.append(current_section)

            # Start new section
            section_num = header_match.group(1)
            title = header_match.group(2).strip()
            current_section = {
                "page": current_page,
                "section": section_num,
                "title": title,
                "text": "",
            }
            current_lines = [line]
        elif current_section is not None:
            current_lines.append(line)
        else:
            # Before any section header — might be preamble/metadata
            pass

    # Save last section
    if current_section and current_lines:
        current_section["text"] = "\n".join(current_lines).strip()
        if len(current_section["text"]) > 50:
            sections.append(current_section)

    return sections


# ─────────────────────────────────────────────────────────────────────────────
# LLM caller
# ─────────────────────────────────────────────────────────────────────────────

def call_llm_for_template(
    prompt: str,
    system_prompt: str = SYSTEM_PROMPT,
    max_retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Call the LLM with the template prompt and parse the JSON response.
    Uses Gemini (configured in llm_clients.py) with retry logic.
    """
    from backend.core.config import Settings
    from backend.core.llm_clients import get_gemini_client

    settings = Settings()
    client = get_gemini_client(timeout=settings.GEMINI_TIMEOUT)

    for attempt in range(1, max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=settings.GEMINI_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,  # Deterministic for structured extraction
                max_tokens=16384,
            )

            content = response.choices[0].message.content.strip()

            # Strip markdown code fences if present
            if content.startswith("```"):
                content = re.sub(r'^```(?:json)?\s*\n?', '', content)
                content = re.sub(r'\n?```\s*$', '', content)

            return json.loads(content)

        except json.JSONDecodeError as e:
            print(f"  ⚠️  Attempt {attempt}: JSON parse error: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
        except Exception as e:
            print(f"  ⚠️  Attempt {attempt}: LLM error: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Main extraction pipeline
# ─────────────────────────────────────────────────────────────────────────────

def extract_section(
    section_info: Dict[str, Any],
    dry_run: bool = False,
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """
    Extract a single section: build prompt → call LLM → validate.
    Returns (validated_json, errors).
    """
    prompt = build_section_extraction_prompt(
        section_text=section_info["text"],
        page_number=section_info["page"],
        section_number=section_info["section"],
    )

    if dry_run:
        print(f"  [DRY RUN] Prompt length: {len(prompt)} chars")
        return None, []

    # Call LLM
    result = call_llm_for_template(prompt)
    if result is None:
        return None, ["LLM returned no valid JSON after retries"]

    # Validate against Pydantic
    section, errors = validate_section_json(result)
    if errors:
        return result, errors  # Return raw JSON for debugging

    # Return the raw JSON (not the Pydantic object) for saving
    return result, []


def run_pipeline(
    pages: Optional[List[int]] = None,
    dry_run: bool = False,
    eval_mode: bool = False,
) -> None:
    """Run the full template extraction pipeline."""

    # 1. Read fused markdown
    if not FUSED_MD.exists():
        print(f"❌ {FUSED_MD} not found. Run the extraction pipeline first.")
        sys.exit(1)

    markdown = FUSED_MD.read_text()
    print(f"📄 Loaded {len(markdown):,} chars from {FUSED_MD.name}")

    # 2. Split into sections
    sections = split_into_sections(markdown)
    print(f"📑 Found {len(sections)} sections")

    # Filter by page if requested
    if pages:
        sections = [s for s in sections if s["page"] in pages]
        print(f"📌 Filtered to {len(sections)} sections on pages {pages}")

    # 3. Create output directory
    SECTIONS_DIR.mkdir(parents=True, exist_ok=True)

    # 4. Process each section
    results = []
    errors_total = 0

    for i, section_info in enumerate(sections, 1):
        sid = f"p{section_info['page']}_s{section_info['section']}"
        print(f"\n[{i}/{len(sections)}] Section {section_info['section']} "
              f"(Page {section_info['page']}): {section_info['title'][:60]}")

        result_json, errors = extract_section(section_info, dry_run=dry_run)

        if errors:
            print(f"  ❌ Errors: {'; '.join(errors)}")
            errors_total += 1
            # Save the raw JSON for debugging
            if result_json:
                err_path = SECTIONS_DIR / f"{sid}_ERROR.json"
                err_path.write_text(json.dumps(result_json, indent=2))
                print(f"  💾 Saved raw JSON: {err_path.name}")
        elif result_json:
            # Save validated JSON
            # Try to use section id from the result
            sec_id = result_json.get("id")
            if isinstance(sec_id, dict):
                sec_id = sec_id.get("value", sid)
            sec_id = sec_id or sid

            out_path = SECTIONS_DIR / f"{sec_id}.json"
            out_path.write_text(json.dumps(result_json, indent=2))
            print(f"  ✅ Saved: {out_path.name}")
            results.append(result_json)

    # 5. Summary
    print(f"\n{'='*60}")
    print(f"Extraction complete: {len(results)} valid, {errors_total} errors, "
          f"{len(sections)} total")

    if eval_mode and results:
        _run_eval(results)


def _run_eval(results: List[Dict[str, Any]]) -> None:
    """Compare extracted sections against the golden YAML."""
    golden_path = PROJECT_ROOT / "storage" / "yaml" / "tariff_rules_latest.yaml"
    if not golden_path.exists():
        print("⚠️  Golden YAML not found, skipping eval")
        return

    from backend.models.tariff_rule import TariffRuleset
    golden = TariffRuleset.from_yaml(golden_path)
    golden_ids = {s.id for s in golden.sections}

    extracted_ids = set()
    for r in results:
        sid = r.get("id")
        if isinstance(sid, dict):
            sid = sid.get("value")
        if sid:
            extracted_ids.add(sid)

    matched = golden_ids & extracted_ids
    missing = golden_ids - extracted_ids
    extra = extracted_ids - golden_ids

    precision = len(matched) / len(extracted_ids) if extracted_ids else 0
    recall = len(matched) / len(golden_ids) if golden_ids else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    print(f"\n📊 Eval vs Golden YAML:")
    print(f"   Precision: {precision:.1%} ({len(matched)}/{len(extracted_ids)})")
    print(f"   Recall:    {recall:.1%} ({len(matched)}/{len(golden_ids)})")
    print(f"   F1:        {f1:.1%}")
    if missing:
        print(f"   Missing:   {sorted(missing)}")
    if extra:
        print(f"   Extra:     {sorted(extra)}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract tariff sections using JSON template + LLM"
    )
    parser.add_argument("--pages", type=str, default=None,
                        help="Comma-separated page numbers to process")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show prompts without calling LLM")
    parser.add_argument("--eval", action="store_true",
                        help="Compare results against golden YAML")
    parser.add_argument("--assemble", action="store_true",
                        help="After extraction, assemble into YAML")

    args = parser.parse_args()

    pages = None
    if args.pages:
        pages = [int(p.strip()) for p in args.pages.split(",")]

    run_pipeline(
        pages=pages,
        dry_run=args.dry_run,
        eval_mode=args.eval,
    )

    if args.assemble:
        print("\n🔧 Assembly mode — not yet implemented. Use template_to_yaml.py assemble.")


if __name__ == "__main__":
    main()
