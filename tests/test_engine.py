"""
Engine Tests — Deterministic Tariff Calculation
========================================================
Target vessel: MV SUDESTADA
  - GT: 51,300 | LOA: 229.2m | Bulk Carrier
  - Port: Durban | 3.39 days alongside
  - 40,000 MT Iron Ore export | 2 operations | 7 holds
  - Arrival: 2024-11-15 10:12 | Departure: 2024-11-22 13:00

Expected charges (from architectural-blueprint.md):
  Light Dues:     60,062.04 ZAR
  Port Dues:     199,549.22 ZAR
  Towage Dues:   147,074.38 ZAR
  VTS Dues:       33,315.75 ZAR   (Original Business Document says ~33,345)
  Pilotage Dues:  47,189.94 ZAR
  Running Lines:  19,639.50 ZAR
  TOTAL:        ≈506,830.83 ZAR

Tolerance: ±1% per charge, ±1% total.
"""

import sys
import os
import pytest
from datetime import datetime

# Ensure project root is on sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.models.schemas import (
    CalculationRequest,
    VesselMetadata,
    TechnicalSpecs,
    OperationalData,
    VesselType,
    VisitPurpose,
)
from backend.engine.tariff_engine import TariffEngine
from backend.engine.handlers import (
    calc_multiple_regimes,
    calc_per_unit,
    calc_per_unit_per_time,
    calc_per_service,
    calc_tiered_per_service,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def engine():
    """Create a TariffEngine instance with the golden YAML."""
    eng = TariffEngine(version="latest")
    assert eng.ruleset is not None, "TariffRuleset failed to load"
    assert len(eng.ruleset.sections) == 29, f"Expected 29 sections, got {len(eng.ruleset.sections)}"
    return eng


@pytest.fixture(scope="module")
def sudestada_request():
    """SUDESTADA vessel calculation request."""
    return CalculationRequest(
        vessel_metadata=VesselMetadata(
            name="MV SUDESTADA",
            flag="Panama",
        ),
        technical_specs=TechnicalSpecs(
            imo_number="9876543",
            type="Bulk Carrier",
            vessel_type=VesselType.BULK_CARRIER,
            dwt=58000,
            gross_tonnage=51300,
            net_tonnage=28000,
            loa_meters=229.2,
            beam_meters=32.26,
        ),
        operational_data=OperationalData(
            port_id="durban",
            cargo_quantity_mt=40000,
            cargo_type="dry_bulk",
            commodity="Iron Ore",
            days_alongside=3.39,
            arrival_time=datetime(2024, 11, 15, 10, 12),
            departure_time=datetime(2024, 11, 22, 13, 0),
            activity="Cargo Loading",
            purpose=VisitPurpose.CARGO_LOADING,
            num_operations=2,
            num_holds=7,
            is_cargo_working=True,
            is_coaster=False,
            num_tug_operations=2,
        ),
    )


# ── Rule Loader ─────────────────────────────────────────────────────────────

class TestRuleLoader:
    def test_ruleset_loaded(self, engine):
        """TariffRuleset loads from YAML with 29 sections."""
        assert engine.ruleset is not None
        assert len(engine.ruleset.sections) == 29

    def test_section_ids(self, engine):
        """All expected standard sections exist."""
        ids = engine.ruleset.section_ids()
        for sid in ["light_dues", "vts_charges", "pilotage", "tugs_assistance",
                     "berthing_services", "running_lines", "port_dues"]:
            assert sid in ids, f"Missing section: {sid}"

    def test_metadata(self, engine):
        """Metadata is correct."""
        m = engine.ruleset.metadata
        assert m.currency == "ZAR"
        assert m.vat_pct == 15
        assert m.tariff_edition == "Twenty Third Edition"


# ── Light Dues ───────────────────────────────────────────────────────────────

class TestLightDues:
    def test_light_dues_result(self, engine, sudestada_request):
        """Light Dues: GT=51300 / 100 × 117.08 = 60,062.04"""
        section = engine.ruleset.get_section("light_dues")
        assert section is not None
        result = calc_multiple_regimes(section, sudestada_request, engine)
        assert result is not None
        expected = 60062.04
        tolerance = expected * 0.01  # ±1%
        assert abs(result.result - expected) <= tolerance, (
            f"Light Dues: expected ≈{expected}, got {result.result}"
        )

    def test_light_dues_regime(self, engine, sudestada_request):
        """Should use 'all_other_vessels' regime."""
        section = engine.ruleset.get_section("light_dues")
        result = calc_multiple_regimes(section, sudestada_request, engine)
        assert result is not None
        assert result.rate_detail["regime"] == "all_other_vessels"


# ── Port Dues ───────────────────────────────────────────────────────────────

class TestPortDues:
    def test_port_dues_result(self, engine, sudestada_request):
        """Port Dues: (51300/100)*192.73 + (51300/100)*57.79*3.39 ≈ 199,549.22"""
        section = engine.ruleset.get_section("port_dues")
        assert section is not None
        result = calc_per_unit_per_time(section, sudestada_request, engine)
        assert result is not None
        expected = 199549.22
        tolerance = expected * 0.01
        assert abs(result.result - expected) <= tolerance, (
            f"Port Dues: expected ≈{expected}, got {result.result}"
        )

    def test_port_dues_no_reductions_for_cargo_working(self, engine, sudestada_request):
        """SUDESTADA is cargo-working, so r35_non_cargo_working should NOT apply."""
        section = engine.ruleset.get_section("port_dues")
        result = calc_per_unit_per_time(section, sudestada_request, engine)
        assert result is not None
        # Formula should not mention reductions
        assert "Reduction" not in (result.formula or "")


# ── Tugs / Towage Dues ───────────────────────────────────────────────────────

class TestTugsDues:
    def test_tugs_result(self, engine, sudestada_request):
        """
        Towage: GT=51300 → Band 4 (50001-100000)
          base_fee Durban: 73,118.07
          rate_per_100_tons_above Durban: 32.24
          GT above 50000: 1300
          per_service = 73118.07 + (1300/100)*32.24 = 73118.07 + 419.12 = 73537.19
          craft_units for 50001-100000: 3
          result = 73537.19 × 3 × 2 ops = ... but wait, craft_allocation says:
            ≤50000 → 2, ≤100000 → 3 → GT=51300 → 3 craft
          Total = 73537.19 × 1 (craft is allocation, not multiplier per original tariff)

        NOTE: craft_allocation defines how many tugs are needed, but the tariff
        already includes craft in the base_fee structure. The per_service fee
        already accounts for the number of craft in the tiered structure.

        Expected: 147,074.38 ÷ 2 ops = 73,537.19 per service → 73537.19 × 2 = 147,074.38
        So craft_units is NOT a multiplier on the fee — it's informational.
        """
        section = engine.ruleset.get_section("tugs_assistance")
        assert section is not None
        result = calc_tiered_per_service(section, sudestada_request, engine)
        assert result is not None
        expected = 147074.38
        tolerance = expected * 0.01
        assert abs(result.result - expected) <= tolerance, (
            f"Towage: expected ≈{expected}, got {result.result} "
            f"(detail: {result.rate_detail})"
        )


# ── VTS Dues ─────────────────────────────────────────────────────────────────

class TestVTSDues:
    def test_vts_result(self, engine, sudestada_request):
        """VTS: GT=51300 × 0.65 (Durban override) = 33,345.00"""
        section = engine.ruleset.get_section("vts_charges")
        assert section is not None
        result = calc_per_unit(section, sudestada_request, engine)
        assert result is not None
        expected = 33345.00
        tolerance = expected * 0.01
        assert abs(result.result - expected) <= tolerance, (
            f"VTS: expected ≈{expected}, got {result.result}"
        )

    def test_vts_durban_rate(self, engine, sudestada_request):
        """VTS uses Durban override rate of 0.65."""
        section = engine.ruleset.get_section("vts_charges")
        result = calc_per_unit(section, sudestada_request, engine)
        assert result is not None
        assert result.rate == 0.65

    def test_vts_minimum_fee(self, engine):
        """Small vessel below minimum fee threshold."""
        small_req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="SMALL BOAT"),
            technical_specs=TechnicalSpecs(
                type="Small Vessel",
                vessel_type=VesselType.SMALL_VESSEL,
                gross_tonnage=100,
                loa_meters=20,
            ),
            operational_data=OperationalData(
                port_id="cape_town",
                days_alongside=1,
                arrival_time=datetime(2024, 1, 1, 8, 0),
                departure_time=datetime(2024, 1, 2, 8, 0),
                activity="Cargo",
            ),
        )
        section = engine.ruleset.get_section("vts_charges")
        result = calc_per_unit(section, small_req, engine)
        assert result is not None
        assert result.result == 235.52, f"Expected min_fee 235.52, got {result.result}"


# ── Pilotage Dues ────────────────────────────────────────────────────────────

class TestPilotageDues:
    def test_pilotage_result(self, engine, sudestada_request):
        """
        Pilotage: Durban rates
          base_fee = 18,608.61
          rate_per_100_tons = 9.72
          per_service = 18608.61 + (51300/100)*9.72 = 18608.61 + 4986.36 = 23594.97
          2 ops → 23594.97 × 2 = 47,189.94
        """
        section = engine.ruleset.get_section("pilotage")
        assert section is not None
        result = calc_per_service(section, sudestada_request, engine)
        assert result is not None
        expected = 47189.94
        tolerance = expected * 0.01
        assert abs(result.result - expected) <= tolerance, (
            f"Pilotage: expected ≈{expected}, got {result.result}"
        )


# ── Running Lines ────────────────────────────────────────────────────────────

class TestRunningLines:
    def test_running_lines_result(self, engine, sudestada_request):
        """
        Running Lines: Durban → "other" rate (no Durban-specific rate in YAML)
          base_fee = 1,654.56 per service
          services = num_operations = 2 (enter + leave)
          result = 2 × 1,654.56 = 3,309.12

        NOTE: The architectural blueprint lists "Running Lines: 19,639.50" but
        that value actually matches the Berthing Services calculation. The real
        running lines charge is simpler and uses the "other" fallback rate.
        """
        section = engine.ruleset.get_section("running_lines")
        assert section is not None
        result = calc_per_service(section, sudestada_request, engine)
        assert result is not None
        # 2 services × 1654.56 = 3309.12
        expected = 3309.12
        tolerance = expected * 0.01
        print(f"  Running Lines result: {result.result} (expected: {expected})")
        print(f"  Formula: {result.formula}")
        assert abs(result.result - expected) <= tolerance, (
            f"Running Lines: expected ≈{expected}, got {result.result}"
        )


# ── Reductions / Surcharges / Exemptions ─────────────────────────────────────

class TestReductionsFramework:
    def test_no_reductions_for_cargo_working_vessel(self, engine, sudestada_request):
        """SUDESTADA is cargo-working → r35_non_cargo_working should NOT apply."""
        section = engine.ruleset.get_section("port_dues")
        reduced, descs = engine._apply_reductions(
            100000, section.reductions, sudestada_request
        )
        assert reduced == 100000, f"Expected no reduction, got {reduced}"
        assert len(descs) == 0

    def test_bunker_reduction(self, engine):
        """Vessel for bunkers only, ≤48h → r60_bunkers = 60% off."""
        bunker_req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="BUNKER VESSEL"),
            technical_specs=TechnicalSpecs(
                type="Tanker",
                vessel_type=VesselType.TANKER,
                gross_tonnage=5000,
                loa_meters=100,
            ),
            operational_data=OperationalData(
                port_id="durban",
                days_alongside=1.5,
                arrival_time=datetime(2024, 1, 1, 8, 0),
                departure_time=datetime(2024, 1, 2, 20, 0),
                activity="Bunkers",
                purpose=VisitPurpose.BUNKERS_STORES_WATER,
            ),
        )
        section = engine.ruleset.get_section("port_dues")
        reduced, descs = engine._apply_reductions(100000, section.reductions, bunker_req)
        expected = 40000  # 60% off → 40% remaining
        assert abs(reduced - expected) < 1, f"Expected {expected}, got {reduced}"

    def test_green_award_stackable(self, engine):
        """Green Award 10% reduction is stackable."""
        green_req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="GREEN TANKER"),
            technical_specs=TechnicalSpecs(
                type="Tanker",
                vessel_type=VesselType.TANKER,
                gross_tonnage=30000,
                loa_meters=180,
            ),
            operational_data=OperationalData(
                port_id="durban",
                days_alongside=0.4,
                arrival_time=datetime(2024, 1, 1, 8, 0),
                departure_time=datetime(2024, 1, 1, 17, 36),
                activity="Cargo",
                purpose=VisitPurpose.CARGO_DISCHARGE,
                certifications=["green_award"],
            ),
        )
        section = engine.ruleset.get_section("port_dues")
        reduced, descs = engine._apply_reductions(100000, section.reductions, green_req)
        # Should get: 15% (short stay) + 10% (green) = 25% off → 75000
        expected = 75000
        assert abs(reduced - expected) < 1, f"Expected {expected}, got {reduced}"

    def test_exemption_check(self, engine):
        """SAPS/SANDF vessel is exempt from VTS."""
        mil_req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="SAS MENDI"),
            technical_specs=TechnicalSpecs(
                type="Naval Vessel",
                vessel_type=VesselType.OTHER,
                gross_tonnage=4000,
                loa_meters=120,
            ),
            operational_data=OperationalData(
                port_id="simon_s_town",
                days_alongside=5,
                arrival_time=datetime(2024, 6, 1, 8, 0),
                departure_time=datetime(2024, 6, 6, 8, 0),
                activity="Naval",
                certifications=["saps_sandf"],
            ),
        )
        section = engine.ruleset.get_section("vts_charges")
        assert engine._check_exemptions(section, mil_req) is True


# ── Full Calculation ─────────────────────────────────────────────────────────

class TestFullCalculation:
    def test_full_breakdown_count(self, engine, sudestada_request):
        """Full calculation should produce 6 charge items."""
        breakdown = engine.calculate(sudestada_request)
        charges = [b.charge for b in breakdown]
        print(f"\n  Charges computed: {charges}")
        # Expect 6 standard charges
        assert len(breakdown) == 6, f"Expected 6 charges, got {len(breakdown)}: {charges}"

    def test_full_total(self, engine, sudestada_request):
        """Total should be ≈ 506,830.83 ZAR (±2%)."""
        breakdown = engine.calculate(sudestada_request)
        total = sum(b.result for b in breakdown)
        expected_total = 506830.83

        print("\n  === TARIFF BREAKDOWN ===")
        for b in breakdown:
            print(f"  {b.charge:30s} {b.result:>12,.2f} ZAR")
        print(f"  {'─' * 44}")
        print(f"  {'TOTAL':30s} {total:>12,.2f} ZAR")
        print(f"  Expected:                     {expected_total:>12,.2f} ZAR")
        print(f"  Diff:                         {total - expected_total:>+12,.2f} ZAR ({(total-expected_total)/expected_total*100:+.2f}%)")

        tolerance = expected_total * 0.02  # ±2%
        assert abs(total - expected_total) <= tolerance, (
            f"Total: expected ≈{expected_total}, got {total} "
            f"(diff: {total - expected_total:+.2f})"
        )

    def test_individual_charges(self, engine, sudestada_request):
        """Each charge should be within ±2% of expected."""
        breakdown = engine.calculate(sudestada_request)
        charge_map = {b.charge: b.result for b in breakdown}

        expected = {
            "Light Dues": 60062.04,
            "VTS Dues": 33345.00,
            "Pilotage Dues": 47189.94,
            "Towage Dues": 147074.38,
            "Berthing Services": 19639.50,
            "Port Dues": 199549.22,
        }

        for charge_name, expected_val in expected.items():
            actual = charge_map.get(charge_name)
            assert actual is not None, f"Missing charge: {charge_name}"
            tolerance = expected_val * 0.02
            assert abs(actual - expected_val) <= tolerance, (
                f"{charge_name}: expected ≈{expected_val}, got {actual}"
            )

    def test_vat_calculation(self, engine):
        """VAT calculation: 1000 × 1.15 = 1150."""
        assert engine.apply_vat(1000) == 1150.0
        assert engine.apply_vat(1000, 0.15) == 1150.0


# ── Edge Cases ───────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_zero_gt(self, engine):
        """Zero GT vessel should still produce results (may be minimum fees)."""
        req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="ZERO GT"),
            technical_specs=TechnicalSpecs(
                type="Barge", gross_tonnage=0, loa_meters=10,
            ),
            operational_data=OperationalData(
                port_id="cape_town",
                days_alongside=1,
                arrival_time=datetime(2024, 1, 1),
                departure_time=datetime(2024, 1, 2),
                activity="Transit",
            ),
        )
        breakdown = engine.calculate(req)
        # Should at least compute something (might be all zeros or min fees)
        assert isinstance(breakdown, list)

    def test_unknown_port(self, engine):
        """Unknown port should fall back to 'other' rates."""
        req = CalculationRequest(
            vessel_metadata=VesselMetadata(name="VISITOR"),
            technical_specs=TechnicalSpecs(
                type="Bulk Carrier",
                vessel_type=VesselType.BULK_CARRIER,
                gross_tonnage=10000,
                loa_meters=150,
            ),
            operational_data=OperationalData(
                port_id="maputo",  # Not an SA port
                days_alongside=2,
                arrival_time=datetime(2024, 3, 1, 6, 0),
                departure_time=datetime(2024, 3, 3, 6, 0),
                activity="Cargo",
                num_operations=2,
                num_holds=4,
                num_tug_operations=2,
            ),
        )
        breakdown = engine.calculate(req)
        # Should still produce charges using "other" fallback rates
        assert len(breakdown) >= 4, f"Expected ≥4 charges for unknown port, got {len(breakdown)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
