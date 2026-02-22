"""
Ingestion Eval Node.

Compare extracted rules vs golden YAML (storage/yaml/tariff_rules_golden.yaml),
report precision/recall per charge type.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml

from backend.core.config import settings


def _normalize_charge_name(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_").replace("-", "_")


def load_golden_charges(golden_path: str | Path | None = None) -> Dict[str, Any]:
    """Load golden YAML and return a dict of charge name -> section data."""
    path = Path(golden_path or settings.YAML_DIR) / "tariff_rules_golden.yaml"
    if not path.exists():
        # Fallback to tariff_rules_latest.yaml if golden not present
        path = Path(settings.YAML_DIR) / "tariff_rules_latest.yaml"
    if not path.exists():
        return {}

    with open(path) as f:
        data = yaml.safe_load(f)
    if not data or "sections" not in data:
        return {}
    return {_normalize_charge_name(s.get("name", "")): s for s in data["sections"] if s.get("name")}


def eval_extracted_rules(
    extracted_rules: List[dict],
    golden_path: str | Path | None = None,
) -> Dict[str, float]:
    """
    Compare extracted rules/sections to golden dataset.

    Handles both new schema (id/name) and legacy schema (charge_name).
    Matching is done on normalized id or name against golden section names.

    Returns:
        Dict with keys: precision, recall, f1, extracted_count, golden_count.
    """
    golden = load_golden_charges(golden_path)
    if not golden:
        return {"precision": 0.0, "recall": 0.0}

    # Build extracted names set — support both new (id, name) and legacy (charge_name)
    extracted_names: set[str] = set()
    for r in extracted_rules:
        # Try id first, then name, then charge_name
        raw = r.get("id") or r.get("name") or r.get("charge_name") or ""
        if raw:
            extracted_names.add(_normalize_charge_name(raw))

    golden_names = set(golden.keys())

    true_positives = extracted_names & golden_names
    precision = len(true_positives) / len(extracted_names) if extracted_names else 0.0
    recall = len(true_positives) / len(golden_names) if golden_names else 0.0

    return {
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0,
        "extracted_count": len(extracted_rules),
        "golden_count": len(golden_names),
        "matched": sorted(true_positives),
        "missed": sorted(golden_names - extracted_names),
    }
