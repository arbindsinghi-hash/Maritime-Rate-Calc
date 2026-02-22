"""
Ingestion Evaluation — tests/eval_ingestion.py
=================================================
Compares the golden YAML (storage/yaml/tariff_rules_latest.yaml) against the
**actual tariff PDF** ground truth extracted by hand from:

    storage/pdfs/Port Tariff.pdf  (27 pages, April 2024 – March 2025)

The eval checks:

  1. Structural completeness — do all 6 chargeable sections exist?
  2. Rate accuracy — do YAML rates match the PDF exactly?
  3. Citation correctness — do page numbers match actual PDF locations?
  4. Reduction/exemption coverage — are the documented reductions captured?
  5. Port-rate coverage — are per-port variants present?

Run:  python -m tests.eval_ingestion           (from project root)
  or: python tests/eval_ingestion.py
"""

import sys
import os
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

import yaml

# Ensure project root on path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═════════════════════════════════════════════════════════════════════════════
# PDF-VERIFIED GROUND TRUTH  (every value cross-checked against the PDF)
# ═════════════════════════════════════════════════════════════════════════════
# Source: Port Tariff Book April 2024 – March 2025, 27 pages
# All rates in ZAR, subject to 15% VAT.

GROUND_TRUTH = {
    # ── 1.1 Light Dues (PDF page 5) ──────────────────────────────────────
    "light_dues": {
        "pdf_page": 5,
        "pdf_section": "1.1",
        "rates": {
            "registered_port_rate_per_metre": 24.64,
            "all_other_rate_per_100_tons": 117.08,
        },
        "exemption_count_min": 5,  # SAPS/SANDF, SAMSA, Medical, non-self-prop, anchorage
    },
    # ── 2.1 VTS Charges (PDF page 6) ─────────────────────────────────────
    "vts_charges": {
        "pdf_page": 6,
        "pdf_section": "2.1",
        "rates": {
            "rate_per_gt_all_ports": 0.54,
            "rate_per_gt_durban_saldanha": 0.65,
            "minimum_fee": 235.52,
        },
        "exemption_count_min": 5,
    },
    # ── 3.3 Pilotage (PDF page 7) ────────────────────────────────────────
    "pilotage": {
        "pdf_page": 7,
        "pdf_section": "3.3",
        "port_rates": {
            "richards_bay": {"base_fee": 30960.46, "per_100_tons": 10.93},
            "durban":       {"base_fee": 18608.61, "per_100_tons": 9.72},
            "port_elizabeth": {"base_fee": 8970.00, "per_100_tons": 14.33},
            "cape_town":    {"base_fee": 6342.39, "per_100_tons": 10.20},
            "saldanha":     {"base_fee": 9673.57, "per_100_tons": 13.66},
            "other":        {"base_fee": 6547.45, "per_100_tons": 10.49},
        },
    },
    # ── 3.6 Tugs / Vessel Assistance (PDF page 8) ────────────────────────
    "tugs_assistance": {
        "pdf_page": 8,
        "pdf_section": "3.6",
        "craft_allocation": {
            "up_to_2000": 0.5,
            "2001_to_10000": 1,
            "10001_to_50000": 2,
            "50001_to_100000": 3,
            "above_100000": 4,
        },
        "durban_band_rates": {
            # band → (base_fee, rate_per_100_tons_above)
            "up_to_2000":       (8140.00, None),
            "2001_to_10000":    (12633.99, 268.99),
            "10001_to_50000":   (38494.51, 84.95),
            "50001_to_100000":  (73118.07, 32.24),
            "above_100000":     (93548.13, 23.65),
        },
    },
    # ── 3.8 Berthing Services (PDF page 9, bottom) ───────────────────────
    "berthing_services": {
        "pdf_page": 9,
        "pdf_section": "3.8",
        "port_rates": {
            "richards_bay":   {"base_fee": 3175.89, "per_100_tons": 13.46},
            "port_elizabeth": {"base_fee": 3838.62, "per_100_tons": 18.72},
            "cape_town":      {"base_fee": 3052.33, "per_100_tons": 14.92},
            "saldanha":       {"base_fee": 4006.34, "per_100_tons": 16.97},
            "other":          {"base_fee": 2801.91, "per_100_tons": 13.68},
        },
    },
    # ── 3.9 Running Lines (PDF page 10) ──────────────────────────────────
    "running_lines": {
        "pdf_page": 10,
        "pdf_section": "3.9",
        "port_rates": {
            "port_elizabeth": {"fee_per_service": 2266.73},
            "cape_town":      {"fee_per_service": 2370.84},
            "saldanha":       {"fee_per_service": 2085.59},
            "other":          {"fee_per_service": 1654.56},
        },
    },
    # ── 4.1.1 Port Dues (PDF page 11) ────────────────────────────────────
    "port_dues": {
        "pdf_page": 11,
        "pdf_section": "4.1.1",
        "rates": {
            "base_rate_per_100_tons": 192.73,
            "incremental_rate_per_100_tons_per_24h": 57.79,
            "small_vessel_minimum": 470.98,
        },
        "reductions": {
            "non_cargo_working_35pct": 0.35,
            "bunkers_60pct": 0.60,
            "short_stay_12h_15pct": 0.15,
            "green_award_10pct": 0.10,
        },
    },
}

# Sections we require — the 6 chargeable categories
REQUIRED_SECTIONS = list(GROUND_TRUTH.keys())


# ═════════════════════════════════════════════════════════════════════════════
# RESULT TRACKING
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class CheckResult:
    category: str
    check: str
    passed: bool
    detail: str = ""

@dataclass
class EvalReport:
    checks: list = field(default_factory=list)

    def add(self, category: str, check: str, passed: bool, detail: str = ""):
        self.checks.append(CheckResult(category, check, passed, detail))

    @property
    def total(self) -> int:
        return len(self.checks)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.passed)

    @property
    def failed(self) -> int:
        return self.total - self.passed

    @property
    def accuracy(self) -> float:
        return self.passed / self.total if self.total else 0.0


# ═════════════════════════════════════════════════════════════════════════════
# EVALUATION LOGIC
# ═════════════════════════════════════════════════════════════════════════════

def _load_yaml() -> dict:
    path = PROJECT_ROOT / "storage" / "yaml" / "tariff_rules_latest.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Golden YAML not found at {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def _get_section(data: dict, section_id: str) -> dict | None:
    for s in data.get("sections", []):
        if s.get("id") == section_id:
            return s
    return None


def _approx(actual: float | None, expected: float, tol: float = 0.005) -> bool:
    """Check if actual is within tolerance (default 0.5%) of expected."""
    if actual is None:
        return False
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tol


def eval_structural_completeness(data: dict, report: EvalReport):
    """Check that all 6 required sections exist in the YAML."""
    section_ids = [s["id"] for s in data.get("sections", [])]
    for sid in REQUIRED_SECTIONS:
        present = sid in section_ids
        report.add("structure", f"section_{sid}_exists", present,
                    f"{'found' if present else 'MISSING'} in {len(section_ids)} sections")


def eval_metadata(data: dict, report: EvalReport):
    """Check metadata fields."""
    meta = data.get("metadata", {})
    report.add("metadata", "currency_is_ZAR",
               meta.get("currency") == "ZAR",
               f"got {meta.get('currency')}")
    report.add("metadata", "vat_is_15",
               meta.get("vat_pct") == 15,
               f"got {meta.get('vat_pct')}")
    report.add("metadata", "edition_present",
               bool(meta.get("tariff_edition")),
               f"got '{meta.get('tariff_edition', '')}'")


def eval_citation_pages(data: dict, report: EvalReport):
    """Check citation page numbers against PDF-verified locations."""
    for sid, truth in GROUND_TRUTH.items():
        section = _get_section(data, sid)
        if not section:
            report.add("citations", f"{sid}_page", False, "section missing")
            continue
        cite = section.get("citation", {})
        yaml_page = cite.get("page")
        expected_page = truth["pdf_page"]
        ok = yaml_page == expected_page
        report.add("citations", f"{sid}_page",
                    ok, f"YAML page={yaml_page}, PDF page={expected_page}")


def eval_light_dues(data: dict, report: EvalReport):
    """Verify Light Dues rates against PDF page 5."""
    truth = GROUND_TRUTH["light_dues"]
    section = _get_section(data, "light_dues")
    if not section:
        report.add("light_dues", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})
    regimes = calc.get("regimes", [])

    # Check "all_other_vessels" regime rate
    all_other = None
    registered = None
    for r in regimes:
        if r.get("id") == "all_other_vessels":
            all_other = r
        elif r.get("id") == "registered_port":
            registered = r

    if all_other:
        rate = all_other.get("rate_per_100_tons")
        ok = _approx(rate, truth["rates"]["all_other_rate_per_100_tons"])
        report.add("light_dues", "all_other_rate_117.08", ok,
                    f"YAML={rate}, PDF=117.08")
    else:
        report.add("light_dues", "all_other_regime_exists", False, "regime missing")

    if registered:
        rate = registered.get("rate_per_metre")
        ok = _approx(rate, truth["rates"]["registered_port_rate_per_metre"])
        report.add("light_dues", "registered_rate_24.64", ok,
                    f"YAML={rate}, PDF=24.64")
    else:
        report.add("light_dues", "registered_regime_exists", False, "regime missing")

    # Exemptions — PDF page 5 lists these under "Exemptions" heading
    exemptions = section.get("exemptions", [])
    ok = len(exemptions) >= truth["exemption_count_min"]
    report.add("light_dues", f"exemptions_count>={truth['exemption_count_min']}",
               ok, f"found {len(exemptions)}")


def eval_vts(data: dict, report: EvalReport):
    """Verify VTS rates against PDF page 6."""
    truth = GROUND_TRUTH["vts_charges"]
    section = _get_section(data, "vts_charges")
    if not section:
        report.add("vts", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})

    # Default rate
    rate = calc.get("rate_per_gt")
    ok = _approx(rate, truth["rates"]["rate_per_gt_all_ports"])
    report.add("vts", "default_rate_0.54", ok, f"YAML={rate}, PDF=0.54")

    # Durban/Saldanha override
    overrides = calc.get("port_overrides", {})
    durban_rate = None
    saldanha_rate = None
    if isinstance(overrides, dict):
        durban_rate = overrides.get("durban", {}).get("rate_per_gt") if isinstance(overrides.get("durban"), dict) else None
        saldanha_rate = overrides.get("saldanha", {}).get("rate_per_gt") if isinstance(overrides.get("saldanha"), dict) else None

    ok_d = _approx(durban_rate, truth["rates"]["rate_per_gt_durban_saldanha"])
    report.add("vts", "durban_override_0.65", ok_d, f"YAML={durban_rate}, PDF=0.65")

    ok_s = _approx(saldanha_rate, truth["rates"]["rate_per_gt_durban_saldanha"])
    report.add("vts", "saldanha_override_0.65", ok_s, f"YAML={saldanha_rate}, PDF=0.65")

    # Minimum fee
    min_fee = calc.get("minimum_fee") or section.get("minimum_fee")
    # Search deeper
    if not min_fee:
        for s in data.get("sections", []):
            if s.get("id") == "vts_charges":
                min_fee = s.get("minimum_fee") or s.get("calculation", {}).get("minimum_fee")
    ok_m = _approx(min_fee, truth["rates"]["minimum_fee"]) if min_fee else False
    report.add("vts", "minimum_fee_235.52", ok_m, f"YAML={min_fee}, PDF=235.52")


def eval_pilotage(data: dict, report: EvalReport):
    """Verify Pilotage per-port rates against PDF page 7."""
    truth = GROUND_TRUTH["pilotage"]
    section = _get_section(data, "pilotage")
    if not section:
        report.add("pilotage", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})
    port_rates = calc.get("port_rates", {})

    for port_id, expected in truth["port_rates"].items():
        pr = port_rates.get(port_id, {}) if isinstance(port_rates, dict) else {}
        # Base fee
        base = pr.get("base_fee")
        ok_b = _approx(base, expected["base_fee"])
        report.add("pilotage", f"{port_id}_base_fee", ok_b,
                    f"YAML={base}, PDF={expected['base_fee']}")
        # Per 100 tons
        per100 = pr.get("rate_per_100_tons")
        ok_r = _approx(per100, expected["per_100_tons"])
        report.add("pilotage", f"{port_id}_per_100_tons", ok_r,
                    f"YAML={per100}, PDF={expected['per_100_tons']}")


def eval_tugs(data: dict, report: EvalReport):
    """Verify Towage Durban band rates against PDF page 8."""
    truth = GROUND_TRUTH["tugs_assistance"]
    section = _get_section(data, "tugs_assistance")
    if not section:
        report.add("tugs", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})
    bands = calc.get("bands", [])

    # Check that bands exist
    report.add("tugs", "bands_exist", len(bands) >= 4,
               f"found {len(bands)} bands")

    # YAML structure: each band has max_tonnage, base_fee_by_port (dict),
    # rate_per_100_tons_above (dict).  Find the band with max_tonnage=100000
    # (the 50001-100000 band).
    target_band = None
    for band in bands:
        if band.get("max_tonnage") == 100000:
            target_band = band
            break

    if target_band:
        durban_base = target_band.get("base_fee_by_port", {}).get("durban")
        expected_base, expected_per100 = truth["durban_band_rates"]["50001_to_100000"]
        ok_b = _approx(durban_base, expected_base)
        report.add("tugs", "durban_band4_base_73118.07", ok_b,
                    f"YAML={durban_base}, PDF={expected_base}")

        durban_per100 = target_band.get("rate_per_100_tons_above", {}).get("durban")
        ok_r = _approx(durban_per100, expected_per100)
        report.add("tugs", "durban_band4_per100_32.24", ok_r,
                    f"YAML={durban_per100}, PDF={expected_per100}")
    else:
        report.add("tugs", "band_50001_to_100000_exists", False,
                    "no band with max_tonnage=100000 found")


def eval_berthing(data: dict, report: EvalReport):
    """Verify Berthing per-port rates against PDF page 9."""
    truth = GROUND_TRUTH["berthing_services"]
    section = _get_section(data, "berthing_services")
    if not section:
        report.add("berthing", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})
    port_rates = calc.get("port_rates", {})

    for port_id, expected in truth["port_rates"].items():
        pr = port_rates.get(port_id, {}) if isinstance(port_rates, dict) else {}
        base = pr.get("base_fee")
        ok_b = _approx(base, expected["base_fee"])
        report.add("berthing", f"{port_id}_base_fee", ok_b,
                    f"YAML={base}, PDF={expected['base_fee']}")
        per100 = pr.get("rate_per_100_tons")
        ok_r = _approx(per100, expected["per_100_tons"])
        report.add("berthing", f"{port_id}_per_100_tons", ok_r,
                    f"YAML={per100}, PDF={expected['per_100_tons']}")


def eval_running_lines(data: dict, report: EvalReport):
    """Verify Running Lines per-port fees against PDF page 10."""
    truth = GROUND_TRUTH["running_lines"]
    section = _get_section(data, "running_lines")
    if not section:
        report.add("running_lines", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})
    port_rates = calc.get("port_rates", {})

    for port_id, expected in truth["port_rates"].items():
        pr = port_rates.get(port_id, {}) if isinstance(port_rates, dict) else {}
        fee = pr.get("base_fee") or pr.get("fee_per_service")
        ok = _approx(fee, expected["fee_per_service"])
        report.add("running_lines", f"{port_id}_fee", ok,
                    f"YAML={fee}, PDF={expected['fee_per_service']}")


def eval_port_dues(data: dict, report: EvalReport):
    """Verify Port Dues rates & reductions against PDF page 11."""
    truth = GROUND_TRUTH["port_dues"]
    section = _get_section(data, "port_dues")
    if not section:
        report.add("port_dues", "section_exists", False, "MISSING")
        return

    calc = section.get("calculation", {})

    # Base rate
    base = calc.get("base_rate_per_100_tons")
    ok = _approx(base, truth["rates"]["base_rate_per_100_tons"])
    report.add("port_dues", "base_rate_192.73", ok,
               f"YAML={base}, PDF=192.73")

    # Incremental rate
    incr = calc.get("incremental_rate_per_100_tons_per_24h")
    ok = _approx(incr, truth["rates"]["incremental_rate_per_100_tons_per_24h"])
    report.add("port_dues", "incr_rate_57.79", ok,
               f"YAML={incr}, PDF=57.79")

    # Reductions
    reductions = section.get("reductions", [])
    report.add("port_dues", "reductions_exist",
               len(reductions) >= 3, f"found {len(reductions)} reductions")

    # Check specific reduction percentages
    reduction_pcts = set()
    for r in reductions:
        pct = r.get("percentage")
        if pct is not None:
            reduction_pcts.add(pct)

    for name, expected_pct in truth["reductions"].items():
        pct_int = int(expected_pct * 100)
        # Check if this percentage appears in any reduction
        found = expected_pct in reduction_pcts or pct_int in reduction_pcts
        report.add("port_dues", f"reduction_{name}",
                    found, f"looking for {expected_pct} in {sorted(reduction_pcts)}")


def eval_numerical_sanity(data: dict, report: EvalReport):
    """
    Run the actual tariff engine on the SUDESTADA scenario and verify each
    charge against PDF-verified hand calculations.

    Hand calculations (from PDF rates, verified):
      Light Dues:  (51300/100) × 117.08 = 60,062.04
      VTS:         51300 × 0.65 = 33,345.00
      Pilotage:    (18608.61 + (51300/100)*9.72) × 2 = 47,189.94
      Towage:      (73118.07 + (1300/100)*32.24) × 2 = 147,074.38
      Berthing:    (2801.91 + (51300/100)*13.68) × 2 = 19,639.50
      Port Dues:   (51300/100)*(192.73 + 57.79*3.39) ≈ 199,371.35
    """
    try:
        from datetime import datetime
        from backend.models.schemas import (
            CalculationRequest, VesselMetadata, TechnicalSpecs,
            OperationalData, VesselType, VisitPurpose,
        )
        from backend.engine.tariff_engine import TariffEngine

        engine = TariffEngine(version="latest")
        req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="MV SUDESTADA", flag="Panama"),
            technical_specs=TechnicalSpecs(
                imo_number="9876543", type="Bulk Carrier",
                vessel_type=VesselType.BULK_CARRIER,
                dwt=58000, gross_tonnage=51300, net_tonnage=28000,
                loa_meters=229.2, beam_meters=32.26,
            ),
            operational_data=OperationalData(
                port_id="durban", cargo_quantity_mt=40000,
                cargo_type="dry_bulk", commodity="Iron Ore",
                days_alongside=3.39,
                arrival_time=datetime(2024, 11, 15, 10, 12),
                departure_time=datetime(2024, 11, 22, 13, 0),
                activity="Cargo Loading",
                purpose=VisitPurpose.CARGO_LOADING,
                num_operations=2, num_holds=7,
                is_cargo_working=True, is_coaster=False,
                num_tug_operations=2,
            ),
        )
        breakdown = engine.calculate(req)
        charge_map = {b.charge: b for b in breakdown}

        # PDF-verified hand calculations
        expected_charges = {
            "Light Dues":       60062.04,
            "VTS Dues":         33345.00,
            "Pilotage Dues":    47189.94,
            "Towage Dues":      147074.38,
            "Berthing Services": 19639.50,
        }
        # Port Dues: allow ±1% since time rounding may differ
        expected_charges["Port Dues"] = 199371.35

        for name, expected in expected_charges.items():
            actual_b = charge_map.get(name)
            if actual_b is None:
                report.add("numerical", f"engine_{name}_exists", False,
                           f"charge '{name}' not in breakdown")
                continue
            tol = 0.01  # 1%
            ok = abs(actual_b.result - expected) / expected <= tol
            report.add("numerical", f"engine_{name}_within_1pct", ok,
                       f"engine={actual_b.result:.2f}, hand_calc={expected:.2f}, "
                       f"diff={abs(actual_b.result - expected):.2f} ({abs(actual_b.result - expected)/expected*100:.2f}%)")

        # Total
        total = sum(b.result for b in breakdown)
        expected_total = sum(expected_charges.values())
        ok = abs(total - expected_total) / expected_total <= 0.015
        report.add("numerical", "total_within_1.5pct", ok,
                    f"engine={total:.2f}, expected={expected_total:.2f}")

    except Exception as e:
        report.add("numerical", "engine_import", False, str(e))


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════

def run_eval() -> EvalReport:
    data = _load_yaml()
    report = EvalReport()

    eval_structural_completeness(data, report)
    eval_metadata(data, report)
    eval_citation_pages(data, report)
    eval_light_dues(data, report)
    eval_vts(data, report)
    eval_pilotage(data, report)
    eval_tugs(data, report)
    eval_berthing(data, report)
    eval_running_lines(data, report)
    eval_port_dues(data, report)
    eval_numerical_sanity(data, report)

    return report


def print_report(report: EvalReport):
    print()
    print("=" * 78)
    print("  INGESTION EVAL — Golden YAML vs PDF Ground Truth")
    print("=" * 78)
    print()

    current_cat = None
    for c in report.checks:
        if c.category != current_cat:
            current_cat = c.category
            print(f"\n── {current_cat.upper()} {'─' * (60 - len(current_cat))}")
        icon = "✅" if c.passed else "❌"
        print(f"  {icon} {c.check:45s}  {c.detail}")

    print()
    print("─" * 78)
    pct = report.accuracy * 100
    print(f"  TOTAL: {report.passed}/{report.total} checks passed ({pct:.1f}%)")
    print(f"  Precision: {pct:.1f}%    Recall: {report.passed}/{report.total}")
    if report.failed:
        print(f"  ⚠️  {report.failed} checks FAILED — review details above")
    else:
        print("  🎉 All checks passed!")
    print("─" * 78)
    print()

    return {
        "total": report.total,
        "passed": report.passed,
        "failed": report.failed,
        "precision": report.accuracy,
        "recall": report.passed / report.total if report.total else 0.0,
        "f1": report.accuracy,  # precision == recall in this eval
    }


if __name__ == "__main__":
    report = run_eval()
    metrics = print_report(report)

    # Exit non-zero if any check failed
    sys.exit(0 if report.failed == 0 else 1)
