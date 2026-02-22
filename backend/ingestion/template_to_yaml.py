"""
template_to_yaml.py — Pipeline: JSON templates → Pydantic validation → YAML output.

STATUS: LEGACY — This module bridges section_template.py JSON output to YAML.
The canonical ingestion pipeline uses dag.py (LangGraph DAG). This module is
retained for reference and direct JSON-to-YAML conversion use cases.

Pipeline:
    1. Read LLM-filled section JSONs from output/sections/*.json
    2. Validate each against TariffSection Pydantic model
    3. Assemble into a full TariffRuleset
    4. Write storage/yaml/tariff_rules_{version}.yaml

Can also be used to:
    - Validate a single JSON section (--validate path/to/section.json)
    - Convert an existing set of JSONs to YAML (--assemble)
    - Diff against the golden YAML (--diff)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.models.tariff_rule import (  # noqa: E402
    TariffRuleset,
    TariffSection,
    TariffMetadata,
    TariffDefinitions,
)


# ─────────────────────────────────────────────────────────────────────────────
# JSON cleaning: strip template metadata keys before Pydantic validation
# ─────────────────────────────────────────────────────────────────────────────

META_KEYS = {"_instructions", "_type", "_description", "_example", "_allowed",
             "_item_schema", "_default"}


def clean_template_json(data: Any) -> Any:
    """
    Recursively remove template metadata keys and extract 'value' from
    leaf nodes like {"_type": "float|null", "value": 123.45} → 123.45

    Handles two LLM response styles:
      Style A (full template echo):  {"_type": "float", "value": 0.54}
      Style B (value-only wrapper):  {"value": 0.54}
    Both → 0.54
    """
    if isinstance(data, dict):
        # Check if this is a leaf template node:
        #   Style A: has _type and value  (LLM echoed the full template)
        #   Style B: has ONLY "value" key, or "value" + meta keys only
        non_meta_keys = {k for k in data if k not in META_KEYS}
        if "value" in data and (
            "_type" in data                          # Style A
            or non_meta_keys == {"value"}            # Style B: {"value": X}
        ):
            val = data["value"]
            if isinstance(val, dict):
                return clean_template_json(val)
            if isinstance(val, list):
                return [clean_template_json(item) for item in val]
            return val

        # Otherwise, recurse on non-meta keys
        cleaned = {}
        for key, val in data.items():
            if key in META_KEYS:
                continue
            cleaned[key] = clean_template_json(val)
        return cleaned

    if isinstance(data, list):
        return [clean_template_json(item) for item in data]

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Validate a single section JSON against TariffSection
# ─────────────────────────────────────────────────────────────────────────────

def validate_section_json(data: Dict[str, Any]) -> Tuple[Optional[TariffSection], List[str]]:
    """
    Validate a cleaned JSON dict against TariffSection.
    Returns (section, errors). If errors is non-empty, section may be None.
    """
    errors: List[str] = []

    # Clean template metadata if present
    cleaned = clean_template_json(data)

    # Remove null values to let Pydantic defaults work
    cleaned = _strip_nulls(cleaned)

    # Auto-generate 'id' from 'name' if LLM left id null/empty
    if "id" not in cleaned and "name" in cleaned:
        import re
        name = cleaned["name"]
        cleaned["id"] = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')

    try:
        section = TariffSection(**cleaned)
        return section, []
    except Exception as e:
        errors.append(str(e))
        return None, errors


def _strip_nulls(d: Any) -> Any:
    """Remove keys with None values (let Pydantic defaults apply)."""
    if isinstance(d, dict):
        return {k: _strip_nulls(v) for k, v in d.items() if v is not None}
    if isinstance(d, list):
        return [_strip_nulls(item) for item in d]
    return d


# ─────────────────────────────────────────────────────────────────────────────
# Assemble full TariffRuleset from parts
# ─────────────────────────────────────────────────────────────────────────────

def assemble_ruleset(
    metadata_json: Dict[str, Any],
    definitions_json: Dict[str, Any],
    section_jsons: List[Dict[str, Any]],
) -> Tuple[Optional[TariffRuleset], List[str]]:
    """
    Assemble a full TariffRuleset from metadata, definitions, and section JSONs.
    All inputs should be cleaned (no template metadata keys).
    """
    errors: List[str] = []

    # Clean all parts
    meta_clean = clean_template_json(metadata_json)
    defs_clean = clean_template_json(definitions_json)
    meta_clean = _strip_nulls(meta_clean)
    defs_clean = _strip_nulls(defs_clean)

    # Validate sections individually (collect errors)
    validated_sections: List[TariffSection] = []
    for i, sj in enumerate(section_jsons):
        section, sec_errors = validate_section_json(sj)
        if sec_errors:
            sid = sj.get("id", {})
            if isinstance(sid, dict):
                sid = sid.get("value", f"section_{i}")
            errors.append(f"Section '{sid}': {'; '.join(sec_errors)}")
        elif section:
            validated_sections.append(section)

    if errors:
        return None, errors

    # Assemble
    try:
        ruleset = TariffRuleset(
            metadata=TariffMetadata(**meta_clean),
            definitions=TariffDefinitions(**defs_clean),
            sections=validated_sections,
        )
        return ruleset, []
    except Exception as e:
        errors.append(f"Assembly error: {e}")
        return None, errors


# ─────────────────────────────────────────────────────────────────────────────
# YAML output
# ─────────────────────────────────────────────────────────────────────────────

def _serialize_for_yaml(obj: Any) -> Any:
    """Recursively convert enums, dates, and other non-serializable types."""
    from enum import Enum
    from datetime import date as date_type

    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, date_type):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize_for_yaml(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize_for_yaml(item) for item in obj]
    return obj


def ruleset_to_yaml(ruleset: TariffRuleset) -> str:
    """Convert a validated TariffRuleset to YAML string."""
    # Use model_dump to get a clean dict, then YAML-serialize
    data = ruleset.model_dump(mode="python", exclude_none=True)
    data = _serialize_for_yaml(data)

    return yaml.dump(
        data,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        width=120,
    )


def save_ruleset_yaml(ruleset: TariffRuleset, output_path: Path) -> None:
    """Validate and save a TariffRuleset as YAML."""
    yaml_str = ruleset_to_yaml(ruleset)

    # Add header comment
    header = (
        "# =============================================================================\n"
        "# TARIFF RULES — Auto-generated from LLM template extraction\n"
        "# =============================================================================\n"
        f"# Sections: {len(ruleset.sections)}\n"
        f"# Validated by: TariffRuleset Pydantic model\n"
        "# =============================================================================\n\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(header + yaml_str)
    print(f"✅ Saved: {output_path} ({len(ruleset.sections)} sections)")


# ─────────────────────────────────────────────────────────────────────────────
# Diff against golden YAML
# ─────────────────────────────────────────────────────────────────────────────

def diff_against_golden(
    generated: TariffRuleset,
    golden_path: Path,
) -> Dict[str, Any]:
    """
    Compare a generated TariffRuleset against the golden YAML.
    Returns a report dict with per-section comparison.
    """
    golden = TariffRuleset.from_yaml(golden_path)

    report = {
        "golden_sections": len(golden.sections),
        "generated_sections": len(generated.sections),
        "missing_sections": [],
        "extra_sections": [],
        "matched_sections": [],
        "rate_mismatches": [],
    }

    golden_ids = {s.id for s in golden.sections}
    gen_ids = {s.id for s in generated.sections}

    report["missing_sections"] = sorted(golden_ids - gen_ids)
    report["extra_sections"] = sorted(gen_ids - golden_ids)

    # Compare matched sections
    for gid in sorted(golden_ids & gen_ids):
        g_sec = golden.get_section(gid)
        n_sec = generated.get_section(gid)
        if g_sec and n_sec:
            match_info = {"id": gid, "name_match": g_sec.name == n_sec.name}
            # Compare calculation type
            match_info["type_match"] = g_sec.calculation.type == n_sec.calculation.type
            report["matched_sections"].append(match_info)

    return report


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Template JSON → Validated YAML pipeline")
    sub = parser.add_subparsers(dest="command")

    # validate: check a single section JSON
    val_p = sub.add_parser("validate", help="Validate a single section JSON")
    val_p.add_argument("json_file", type=Path)

    # assemble: combine metadata + definitions + sections → YAML
    asm_p = sub.add_parser("assemble", help="Assemble sections into YAML")
    asm_p.add_argument("--sections-dir", type=Path, required=True,
                       help="Directory containing section JSON files")
    asm_p.add_argument("--metadata", type=Path, required=True,
                       help="Metadata JSON file")
    asm_p.add_argument("--definitions", type=Path, required=True,
                       help="Definitions JSON file")
    asm_p.add_argument("--output", type=Path, default=None,
                       help="Output YAML path")

    # diff: compare generated vs golden
    diff_p = sub.add_parser("diff", help="Diff generated YAML against golden")
    diff_p.add_argument("generated", type=Path)
    diff_p.add_argument("--golden", type=Path,
                        default=PROJECT_ROOT / "storage/yaml/tariff_rules_latest.yaml")

    args = parser.parse_args()

    if args.command == "validate":
        data = json.loads(args.json_file.read_text())
        section, errors = validate_section_json(data)
        if errors:
            print("❌ Validation FAILED:")
            for e in errors:
                print(f"   {e}")
            sys.exit(1)
        else:
            print(f"✅ Valid: {section.id} ({section.name}), type={section.calculation.type}")

    elif args.command == "assemble":
        # Load metadata & definitions
        meta = json.loads(args.metadata.read_text())
        defs = json.loads(args.definitions.read_text())

        # Load all section JSONs
        section_files = sorted(args.sections_dir.glob("*.json"))
        sections = [json.loads(f.read_text()) for f in section_files]
        print(f"Loading {len(sections)} sections from {args.sections_dir}")

        ruleset, errors = assemble_ruleset(meta, defs, sections)
        if errors:
            print("❌ Assembly FAILED:")
            for e in errors:
                print(f"   {e}")
            sys.exit(1)

        output = args.output or (PROJECT_ROOT / "storage/yaml/tariff_rules_generated.yaml")
        save_ruleset_yaml(ruleset, output)

    elif args.command == "diff":
        gen_ruleset = TariffRuleset.from_yaml(args.generated)
        report = diff_against_golden(gen_ruleset, args.golden)
        print(json.dumps(report, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
