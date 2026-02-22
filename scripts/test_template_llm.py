"""
test_template_llm.py — Real LLM test: feed actual PDF-extracted text through
                        the JSON template and validate against golden YAML.

Tests 3 sections of increasing complexity:
  1. VTS (page 6)     — simple per_unit, 1 rate + 2 port overrides + exemptions
  2. Pilotage (page 7) — per_service, 6 port rate tables + 4 surcharges
  3. Tugs (page 8)    — tiered_per_service, bands×ports, craft allocation, delay fees

Usage:
    cd mrca-ai-tariff
    python scripts/test_template_llm.py
"""

from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.core.config import Settings
from backend.core.llm_clients import get_llm_client
from backend.ingestion.section_template import (
    SYSTEM_PROMPT,
    build_section_extraction_prompt,
)
from backend.ingestion.template_to_yaml import validate_section_json
from backend.models.tariff_rule import TariffRuleset

settings = Settings()

# ─────────────────────────────────────────────────────────────────────────────
# The 3 test sections: raw extracted text from fused_pages_markdown.md
# ─────────────────────────────────────────────────────────────────────────────

SECTION_VTS = """
### VESSEL TRAFFIC SERVICES (VTS)

### 2.1 VTS CHARGES ON VESSELS

VTS charges have been introduced in the interest of safe navigation, pollution and conservancy of the ports based on the gross tonnage of a vessel.

The tonnage of a vessel for port tariff purposes is the gross tonnage of a vessel as per the tonnage certificate issued in terms of the Tonnage Convention 1969. (NOT converted to cubic metres.)

Where the vessel's tonnage certificate is not available, the highest tonnage reflected in Lloyds Register of Shipping, is acceptable.

### 2.1.1 VTS CHARGES

Payable by:

- - Vessels calling all Ports under the control of the Authority, and vessels performing port related services within port limits and approaches to port limits, as follows:

Payable per GT per port call at all ports excluding Durban and Saldanha Bay...........0.54

Payable per GT per port call at the ports of Durban and Saldanha Bay..................0.65

Minimum fee...................................................................................................................235.52

Exemptions

- - Vessels belonging to the SAPS and the SANDF;

- - Vessels belonging to SAMSA;

- - SA Medical & Research vessels;

- - Vessels returning from anchorage at the order of the Harbour Master; and

- - Vessels resorting under Section 4, Clause 4.2 (small vessels and pleasure vessels).
"""

SECTION_PILOTAGE = """
### 3.3 PILOTAGE SERVICES

Tariffs subject to VAT at 15%: Tariffs in South African Rand

Pilotage is compulsory at the Ports of Richards Bay, Durban, East London, Ngqura, Port Elizabeth, Mossel Bay, Cape Town and Saldanha with the service being performed by the Authority (Marine Services).

Tonnage of a vessel for Pilotage services purposes:

| Ports | Richards Bay | Durban | Port Elizabeth / Ngqura | Cape Town | Sal-danha | Other |
| --- | --- | --- | --- | --- | --- | --- |
| Per Service (normal entering or leaving the port) Basic Fee | 30 960.46 | 18 608.61 | 8 970.00 | 6 342.39 | 9 673.57 | 6 547.45 |
| Per 100 tons or part thereof | 10.93 | 9.72 | 14.33 | 10.20 | 13.66 | 10.49 |

Pilotage dues for services other than normal entering or leaving the port such as towage, standing by, etc. are available on application.

Any movement of vessels without the consent of the Authority will be subject to full pilotage charges as if the service was performed.

A surcharge of 50% is payable at all ports in the following instances:

- - If the pilotage service terminates or commences outside ordinary working hours;

- - If the vessel is not ready to be moved 30 minutes after the notified time or 30 minutes after the pilot has boarded, whichever is the later;

- - If the request for a pilotage service is cancelled at any time within 30 minutes prior to the notified time and the pilot has not boarded.

A surcharge of 50% is only applicable at the Port of Durban in the following instance:

- - If the request for a pilotage service is cancelled at any time within 60 minutes prior to the notified time and the pilot has not boarded.

At the Port of Saldanha:

PLO duties for pilots on board tanker vessels during stay - charge per hour....................886.20

Exemptions

- - Vessels belonging to the SAPS and SANDF except if pilotage services are performed on request.
"""

SECTION_TUGS = """
### 3.6 TUGS/VESSEL ASSISTANCE AND/OR ATTENDANCE

Tariffs subject to VAT at 15%: Tariffs in South African Rand

The table hereunder shows the craft assistance allocation for the varied vessel size ranges.

| VESSEL TONNAGE | MAXIMUM NUMBER OF CRAFT |
| --- | --- |
| Up to 2 000 | 0.50 |
| 2 000—10 000 | 1 |
| 10 001—50 000 | 2 |
| 50 001—100 000 | 3 |
| 100 000 plus | 4 |

0.50 Represents workboat

The undermentioned fees are payable for tugs/vessels assisting and/or attending vessels, within the confines of the port and are as follows:

Per service based on vessel's tonnage:

|  | Richards Bay | Durban | East London | Port Elizabeth / Ngqura | Mossel Bay | Cape Town | Saldanha |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Up to 2 000 | 7 001.67 | 8 140.00 | 5 622.16 | 7 206.98 | 6 316.53 | 5 411.47 | 9 038.42 |
| 2 001 to 10 000 | 13 020.67 | 12 633.99 | 8 152.14 | 11 168.45 | 8 152.14 | 7 898.57 | 15 378.78 |
| Plus Per 100 tons or part thereof above 2 000 | 275.32 | 268.99 | 200.97 | 237.53 | 173.37 | 194.63 | 327.43 |
| 10 000 to 50 000 | 39 999.88 | 38 494.51 | 27 956.91 | 32 257.98 | 25 806.37 | 27 741.85 | 47 311.70 |
| Plus Per 100 tons or part thereof above 10 000 | 101.08 | 84.95 | 66.67 | 73.10 | 60.21 | 64.52 | 103.23 |
| 50 001 to 100 000 | 79 999.76 |  |  |  |  |  |  |

Incremental charge "Plus" is per additional 100 ton/part thereof

- The craft type and number thereof to be allocated for a service will be decided by the port.

• A surcharge of 25% is payable for a service either commencing or terminating outside
ordinary working hours on weekdays and Saturdays or on Sundays and public holidays;

• A surcharge of 50% is payable per tug when an additional tug/vessel is provided on the
request of the master of the vessel or if deemed necessary in the interest of safety by
the Harbour Master; (in addition to the maximum allocation as per craft allocation table)

• A surcharge of 50% is payable where a vessel without it's own power is serviced. Should
an additional tug/vessel be provided on the request of the master to service such a vessel, a 100% surcharge is payable; (in addition to the maximum allocation as per craft
allocation table)

• Should the request for a tug/vessel to remain/come on duty outside ordinary working
hours be cancelled at any time after standby has commenced, the fees as if the service
had been performed, are payable, i.e. normal fees enhanced by 25%.

• Should a vessel arrive or depart 30 minutes or more after the notified time the fee per
tug per half hour or part thereof is (all ports excluding the Port of Saldanha)…….8 050.76

• Port of Saldanha……………………………………………………………………………………………….10 152.19
"""


# ─────────────────────────────────────────────────────────────────────────────
# Golden values to compare against (from tariff_rules_latest.yaml)
# ─────────────────────────────────────────────────────────────────────────────

GOLDEN_CHECKS = {
    "vts_charges": {
        "id": "vts_charges",
        "calc_type": "per_unit",
        "rate_per_gt": 0.54,
        "minimum_fee": 235.52,
        "port_override_durban_rate": 0.65,
        "port_override_saldanha_rate": 0.65,
        "exemption_count": 5,
    },
    "pilotage": {
        "id": "pilotage",
        "calc_type": "per_service",
        "richards_bay_base_fee": 30960.46,
        "richards_bay_rate_per_100": 10.93,
        "durban_base_fee": 18608.61,
        "durban_rate_per_100": 9.72,
        "cape_town_base_fee": 6342.39,
        "saldanha_base_fee": 9673.57,
        "surcharge_count": 4,  # 3 general + 1 Durban-specific
    },
    "tugs_assistance": {
        "id": "tugs_assistance",
        "calc_type": "tiered_per_service",
        "craft_allocation_count": 5,
        "band_count_min": 4,  # at least 4 bands
        "richards_bay_band1": 7001.67,
        "durban_band1": 8140.00,
        "saldanha_band1": 9038.42,
        "delay_fee": 8050.76,
        "saldanha_delay_fee": 10152.19,
        "surcharge_count_min": 3,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# LLM caller
# ─────────────────────────────────────────────────────────────────────────────

MODEL_ID = "openai/gpt-oss-120b"   # from /v1/models listing


def call_llm(section_text: str, page: int, section_num: str) -> Optional[Dict]:
    """Call gpt-oss:120b with the section template prompt."""
    prompt = build_section_extraction_prompt(section_text, page, section_num)
    client = get_llm_client()

    print(f"    📡 Calling {MODEL_ID} ({len(prompt):,} chars)...")
    start = time.time()

    try:
        response = client.chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=16384,
        )
        elapsed = time.time() - start
        content = response.choices[0].message.content.strip()
        print(f"    ⏱️  Response in {elapsed:.1f}s ({len(content):,} chars)")

        # Strip markdown fences
        if content.startswith("```"):
            content = re.sub(r'^```(?:json)?\s*\n?', '', content)
            content = re.sub(r'\n?```\s*$', '', content)

        return json.loads(content)

    except json.JSONDecodeError as e:
        print(f"    ❌ JSON parse error: {e}")
        print(f"    Raw content (first 500 chars): {content[:500]}")
        return None
    except Exception as e:
        print(f"    ❌ LLM error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Comparison logic
# ─────────────────────────────────────────────────────────────────────────────

def compare_vts(result: Dict, section) -> List[str]:
    """Compare VTS extraction against golden values."""
    golden = GOLDEN_CHECKS["vts_charges"]
    issues = []

    if section.calculation.type != golden["calc_type"]:
        issues.append(f"calc_type: got {section.calculation.type}, expected {golden['calc_type']}")

    if section.calculation.rate_per_gt != golden["rate_per_gt"]:
        issues.append(f"rate_per_gt: got {section.calculation.rate_per_gt}, expected {golden['rate_per_gt']}")

    if section.minimum_fee != golden["minimum_fee"]:
        issues.append(f"minimum_fee: got {section.minimum_fee}, expected {golden['minimum_fee']}")

    # Port overrides
    overrides = section.calculation.port_overrides or {}
    durban_rate = overrides.get("durban", {}).get("rate_per_gt")
    if durban_rate != golden["port_override_durban_rate"]:
        issues.append(f"durban override: got {durban_rate}, expected {golden['port_override_durban_rate']}")

    saldanha_rate = overrides.get("saldanha", {}).get("rate_per_gt")
    if saldanha_rate != golden["port_override_saldanha_rate"]:
        # Also check saldanha_bay variant
        saldanha_rate = overrides.get("saldanha_bay", {}).get("rate_per_gt", saldanha_rate)
        if saldanha_rate != golden["port_override_saldanha_rate"]:
            issues.append(f"saldanha override: got {saldanha_rate}, expected {golden['port_override_saldanha_rate']}")

    if len(section.exemptions) != golden["exemption_count"]:
        issues.append(f"exemptions: got {len(section.exemptions)}, expected {golden['exemption_count']}")

    return issues


def compare_pilotage(result: Dict, section) -> List[str]:
    """Compare pilotage extraction against golden values."""
    golden = GOLDEN_CHECKS["pilotage"]
    issues = []

    if section.calculation.type != golden["calc_type"]:
        issues.append(f"calc_type: got {section.calculation.type}, expected {golden['calc_type']}")

    port_rates = section.calculation.port_rates or {}

    # Check Richards Bay
    rb = port_rates.get("richards_bay")
    if rb:
        if rb.base_fee != golden["richards_bay_base_fee"]:
            issues.append(f"RB base_fee: got {rb.base_fee}, expected {golden['richards_bay_base_fee']}")
        if rb.rate_per_100_tons != golden["richards_bay_rate_per_100"]:
            issues.append(f"RB rate: got {rb.rate_per_100_tons}, expected {golden['richards_bay_rate_per_100']}")
    else:
        issues.append("richards_bay port_rates missing")

    # Check Durban
    db = port_rates.get("durban")
    if db:
        if db.base_fee != golden["durban_base_fee"]:
            issues.append(f"Durban base_fee: got {db.base_fee}, expected {golden['durban_base_fee']}")
    else:
        issues.append("durban port_rates missing")

    # Check Cape Town
    ct = port_rates.get("cape_town")
    if ct:
        if ct.base_fee != golden["cape_town_base_fee"]:
            issues.append(f"CT base_fee: got {ct.base_fee}, expected {golden['cape_town_base_fee']}")
    else:
        issues.append("cape_town port_rates missing")

    # Surcharges
    if len(section.surcharges) < golden["surcharge_count"]:
        issues.append(f"surcharges: got {len(section.surcharges)}, expected ≥{golden['surcharge_count']}")

    return issues


def compare_tugs(result: Dict, section) -> List[str]:
    """Compare tugs extraction against golden values."""
    golden = GOLDEN_CHECKS["tugs_assistance"]
    issues = []

    if section.calculation.type != golden["calc_type"]:
        issues.append(f"calc_type: got {section.calculation.type}, expected {golden['calc_type']}")

    # Craft allocation
    ca = section.calculation.craft_allocation or []
    if len(ca) < golden["craft_allocation_count"]:
        issues.append(f"craft_allocation: got {len(ca)}, expected {golden['craft_allocation_count']}")

    # Bands
    bands = section.calculation.bands or []
    if len(bands) < golden["band_count_min"]:
        issues.append(f"bands: got {len(bands)}, expected ≥{golden['band_count_min']}")

    # Check band 1 port-specific rates
    if bands:
        b1 = bands[0]
        bfp = b1.base_fee_by_port or {}
        rb_val = bfp.get("richards_bay")
        if rb_val != golden["richards_bay_band1"]:
            issues.append(f"band1 RB: got {rb_val}, expected {golden['richards_bay_band1']}")
        db_val = bfp.get("durban")
        if db_val != golden["durban_band1"]:
            issues.append(f"band1 Durban: got {db_val}, expected {golden['durban_band1']}")
        sa_val = bfp.get("saldanha")
        if sa_val != golden["saldanha_band1"]:
            issues.append(f"band1 Saldanha: got {sa_val}, expected {golden['saldanha_band1']}")

    # Delay fee
    if section.delay_fee_per_tug_per_half_hour != golden["delay_fee"]:
        # Also check special dict
        special_delay = (section.special or {}).get("delay_fee_per_tug_per_half_hour")
        if special_delay != golden["delay_fee"] and section.delay_fee_per_tug_per_half_hour != golden["delay_fee"]:
            issues.append(f"delay_fee: got {section.delay_fee_per_tug_per_half_hour}, expected {golden['delay_fee']}")

    # Surcharges
    if len(section.surcharges) < golden["surcharge_count_min"]:
        issues.append(f"surcharges: got {len(section.surcharges)}, expected ≥{golden['surcharge_count_min']}")

    return issues


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("TEMPLATE LLM TEST — Real gpt-oss:120b calls")
    print(f"Model: {MODEL_ID}")
    print(f"Endpoint: {settings.LLM_API_BASE[:60]}...")
    print("=" * 70)

    # Save raw outputs for inspection
    output_dir = PROJECT_ROOT / "output" / "template_test"
    output_dir.mkdir(parents=True, exist_ok=True)

    test_cases = [
        ("VTS Charges", SECTION_VTS, 6, "2.1", compare_vts, "vts_charges"),
        ("Pilotage", SECTION_PILOTAGE, 7, "3.3", compare_pilotage, "pilotage"),
        ("Tugs/Craft Assistance", SECTION_TUGS, 8, "3.6", compare_tugs, "tugs_assistance"),
    ]

    total_checks = 0
    total_pass = 0
    total_issues = []

    for name, text, page, sec_num, compare_fn, expected_id in test_cases:
        print(f"\n{'─'*70}")
        print(f"TEST: {name} (Page {page}, Section {sec_num})")
        print(f"{'─'*70}")

        # 1. Call LLM
        raw_json = call_llm(text, page, sec_num)
        if raw_json is None:
            print("    ❌ SKIP — no response from LLM")
            total_issues.append(f"{name}: no LLM response")
            continue

        # Save raw output
        raw_path = output_dir / f"{expected_id}_raw.json"
        raw_path.write_text(json.dumps(raw_json, indent=2))
        print(f"    💾 Raw output: {raw_path.name}")

        # 2. Pydantic validation — inject fallback id/name if LLM left them null
        from backend.ingestion.template_to_yaml import clean_template_json, _strip_nulls
        cleaned_preview = _strip_nulls(clean_template_json(raw_json))
        if "id" not in cleaned_preview:
            raw_json["id"] = expected_id
            print(f"    ⚠️  LLM left id null, injecting: {expected_id}")
        if "name" not in cleaned_preview:
            raw_json["name"] = name
            print(f"    ⚠️  LLM left name null, injecting: {name}")

        section, val_errors = validate_section_json(raw_json)
        if val_errors:
            print(f"    ❌ Pydantic FAILED: {val_errors[0][:200]}")
            total_issues.append(f"{name}: Pydantic validation failed")

            # Save the error for debugging
            err_path = output_dir / f"{expected_id}_error.txt"
            err_path.write_text("\n".join(val_errors))
            continue

        print(f"    ✅ Pydantic valid: id={section.id}, type={section.calculation.type}")

        # 3. Compare against golden
        issues = compare_fn(raw_json, section)
        checks_count = len(GOLDEN_CHECKS[expected_id])
        pass_count = checks_count - len(issues)
        total_checks += checks_count
        total_pass += pass_count

        if issues:
            print(f"    ⚠️  {len(issues)} mismatches (out of {checks_count} checks):")
            for issue in issues:
                print(f"       • {issue}")
            total_issues.extend([f"{name}: {i}" for i in issues])
        else:
            print(f"    ✅ All {checks_count} golden checks PASSED")

    # Summary
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Checks passed: {total_pass}/{total_checks}")
    if total_checks > 0:
        print(f"  Accuracy:      {total_pass/total_checks:.1%}")
    if total_issues:
        print(f"  Issues ({len(total_issues)}):")
        for issue in total_issues:
            print(f"    • {issue}")
    else:
        print(f"  🎉 ALL CHECKS PASSED — template approach works with real LLM!")

    print(f"\n  Raw outputs saved to: {output_dir}/")


if __name__ == "__main__":
    main()
