"""
API Integration Tests
=====================
Uses FastAPI TestClient with real JSONL-based audit store.
No mocks, no fakes — tests run against real code.
(Exception: Chat tests mock the Gemini API call since no API key in CI.)

Tests cover:
  - POST /api/v1/calculate
  - POST /api/v1/chat
  - POST /api/v1/ingest
  - GET  /api/v1/audit/{id}
  - 422 validation errors
  - GET  /api/v1/citation/{name}
  - GET  /api/v1/citation/{name}/page
  - GET  /api/v1/citations
  - GET  /health
"""
import os
import sys
import pytest
from unittest.mock import patch

# ── Ensure project root is on sys.path ───────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fastapi.testclient import TestClient         # noqa: E402
from backend.main import app                      # noqa: E402
from backend.core.audit_store import audit_store  # noqa: E402


# ══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def clean_audit_store():
    """Clear audit store before each test for isolation."""
    audit_store.clear()
    yield
    audit_store.clear()


@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient — no DB dependency, all real code."""
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def sudestada_payload() -> dict:
    """SUDESTADA vessel payload matching the golden calculation."""
    return {
        "vessel_metadata": {
            "name": "SUDESTADA",
            "flag": "Liberia",
        },
        "technical_specs": {
            "type": "Bulk Carrier",
            "vessel_type": "bulk_carrier",
            "gross_tonnage": 51300.0,
            "loa_meters": 229.2,
        },
        "operational_data": {
            "port_id": "durban",
            "cargo_quantity_mt": 40000.0,
            "cargo_type": "Iron Ore",
            "commodity": "iron_ore",
            "days_alongside": 3.39,
            "arrival_time": "2024-11-15T10:12:00",
            "departure_time": "2024-11-22T13:00:00",
            "activity": "Export",
            "purpose": "cargo_loading",
            "num_operations": 2,
            "num_holds": 7,
            "is_coaster": False,
            "num_tug_operations": 2,
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

class TestHealth:
    def test_root(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "message" in r.json()

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


# ══════════════════════════════════════════════════════════════════════════════
# POST /calculate
# ══════════════════════════════════════════════════════════════════════════════

class TestCalculate:
    def test_calculate_sudestada(self, client, sudestada_payload):
        """Full SUDESTADA calculation: total ~ 506,830 ZAR (+/-1%)."""
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        assert r.status_code == 200
        data = r.json()

        assert data["total_zar"] > 0
        assert data["currency"] == "ZAR"
        assert data["tariff_version"] == "latest"

        # Expect 6 charge items in standard port call
        assert len(data["breakdown"]) == 6

        # Total within 1% of golden value
        expected_total = 506830.83
        assert abs(data["total_zar"] - expected_total) / expected_total < 0.01

    def test_calculate_has_vat(self, client, sudestada_payload):
        """Verify VAT fields are populated in response."""
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        data = r.json()

        assert data["vat_amount"] is not None
        assert data["total_with_vat"] is not None
        assert data["vat_amount"] > 0
        assert data["total_with_vat"] > data["total_zar"]

        # VAT = 15% of total
        expected_vat = round(data["total_zar"] * 0.15, 2)
        assert abs(data["vat_amount"] - expected_vat) < 1.0

    def test_calculate_has_audit_id(self, client, sudestada_payload):
        """Verify audit_id is returned and valid."""
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        data = r.json()
        assert data["audit_id"] is not None
        assert isinstance(data["audit_id"], int)
        assert data["audit_id"] > 0

    def test_calculate_breakdown_has_citations(self, client, sudestada_payload):
        """Each breakdown item should have a citation with page and section."""
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        data = r.json()
        for item in data["breakdown"]:
            assert "citation" in item
            assert "page" in item["citation"]
            assert "section" in item["citation"]
            assert item["citation"]["page"] > 0

    def test_calculate_breakdown_charges(self, client, sudestada_payload):
        """Verify breakdown contains expected charge names."""
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        data = r.json()
        charge_names = {item["charge"] for item in data["breakdown"]}
        expected_charges = {
            "Light Dues",
            "VTS Dues",
            "Pilotage Dues",
            "Towage Dues",
            "Berthing Services",
            "Port Dues",
        }
        assert charge_names == expected_charges


# ══════════════════════════════════════════════════════════════════════════════
# POST /chat
# ══════════════════════════════════════════════════════════════════════════════

def _mock_gemini_extract(message: str, api_key=None) -> dict:
    """
    Simulate Gemini 2.5 Flash extraction for tests.
    Parses the same patterns the real extractor would produce.
    """
    import re
    msg = message.lower()

    gt = None
    gt_match = re.search(r"(\d[\d,]*)\s*(?:gt|gross\s*tonnage|ton)", msg)
    if gt_match:
        gt = float(gt_match.group(1).replace(",", ""))

    port = None
    port_match = re.search(
        r"(?:at|in|port)\s+(durban|cape\s*town|saldanha|richards\s*bay|port\s*elizabeth)",
        msg,
    )
    if port_match:
        port = port_match.group(1).replace(" ", "_")

    vtype = "Bulk Carrier"
    vtype_enum = "bulk_carrier"
    if "tanker" in msg:
        vtype, vtype_enum = "Tanker", "tanker"
    elif "container" in msg:
        vtype, vtype_enum = "Container", "container"
    elif "fishing" in msg:
        vtype, vtype_enum = "Fishing", "fishing_vessel"

    return {
        "vessel_metadata": {"name": "Unknown"},
        "technical_specs": {
            "type": vtype,
            "vessel_type": vtype_enum,
            "gross_tonnage": gt,
            "loa_meters": 200.0,
        },
        "operational_data": {
            "port_id": port,
            "days_alongside": 3.0,
            "arrival_time": "2024-11-15T10:00:00",
            "departure_time": "2024-11-18T10:00:00",
            "activity": "Cargo",
            "num_operations": 2,
            "num_holds": 7,
        },
    }


class TestChat:
    """Chat tests mock the Gemini API call since no API key in CI."""

    @pytest.fixture(autouse=True)
    def _mock_gemini(self):
        with patch("backend.api.endpoints._extract_via_gemini", side_effect=_mock_gemini_extract):
            yield

    def test_chat_basic_query(self, client):
        """NL query returns breakdown with charges."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate dues for a 51300 GT bulk carrier at Durban"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["total_zar"] > 0
        assert len(data["breakdown"]) > 0
        assert data["audit_id"] is not None

    def test_chat_has_vat(self, client):
        """Chat response includes VAT fields."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate dues for a 30000 GT tanker at Durban"},
        )
        data = r.json()
        assert data["vat_amount"] is not None
        assert data["total_with_vat"] is not None

    def test_chat_has_extracted_fields(self, client):
        """Chat response includes the extracted fields dict."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate dues for a 51300 GT bulk carrier at Cape Town"},
        )
        data = r.json()
        assert "extracted_fields" in data
        assert data["extracted_fields"] is not None

    def test_chat_missing_message(self, client):
        """Missing message field triggers 422."""
        r = client.post("/api/v1/chat", json={})
        assert r.status_code == 422

    def test_chat_empty_message(self, client):
        """Empty message triggers 422."""
        r = client.post("/api/v1/chat", json={"message": ""})
        assert r.status_code == 422

    def test_chat_missing_port_returns_422(self, client):
        """Message with GT but no port returns 422 with helpful detail."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate for 30000 GT tanker"},
        )
        assert r.status_code == 422
        assert "port" in r.json()["detail"].lower()

    def test_chat_missing_gt_returns_422(self, client):
        """Message with port but no GT returns 422 with helpful detail."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate dues at Durban"},
        )
        assert r.status_code == 422
        assert "gross_tonnage" in r.json()["detail"].lower()

    def test_chat_extracts_gt(self, client):
        """Verify GT is extracted from message."""
        r = client.post(
            "/api/v1/chat",
            json={"message": "Calculate for 30000 GT vessel at Richards Bay"},
        )
        data = r.json()
        gt = data["extracted_fields"]["technical_specs"]["gross_tonnage"]
        assert gt == 30000.0

    def test_chat_status_endpoint(self, client):
        """GET /chat/status returns gemini_configured flag."""
        r = client.get("/api/v1/chat/status")
        assert r.status_code == 200
        data = r.json()
        assert "gemini_configured" in data
        assert "model" in data


# ══════════════════════════════════════════════════════════════════════════════
# POST /ingest (stub mode)
# ══════════════════════════════════════════════════════════════════════════════

class TestIngest:
    def test_ingest_no_input(self, client):
        """Missing file and file_path returns 400."""
        r = client.post("/api/v1/ingest")
        assert r.status_code == 400

    def test_ingest_stub_mode(self, client):
        """With a file_path, should return a stub response (DAG not wired)."""
        r = client.post("/api/v1/ingest?file_path=storage/pdfs/test.pdf")
        # Should either succeed (if DAG exists) or return stub
        assert r.status_code == 200
        data = r.json()
        assert "status" in data
        assert "job_id" in data


# ══════════════════════════════════════════════════════════════════════════════
# GET /audit/{id}
# ══════════════════════════════════════════════════════════════════════════════

class TestAudit:
    def test_audit_after_calculate(self, client, sudestada_payload):
        """After a calculation, can retrieve the audit log."""
        # First, do a calculation
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        audit_id = r.json()["audit_id"]

        # Then retrieve the audit
        r2 = client.get(f"/api/v1/audit/{audit_id}")
        assert r2.status_code == 200
        data = r2.json()
        assert data["id"] == audit_id
        assert data["vessel_name"] == "SUDESTADA"
        assert data["input_data"] is not None
        assert data["output_data"] is not None
        assert data["tariff_version"] == "latest"

    def test_audit_not_found(self, client):
        """Querying a non-existent audit id returns 404."""
        r = client.get("/api/v1/audit/999999")
        assert r.status_code == 404


# ══════════════════════════════════════════════════════════════════════════════
# Error Handling (422 Validation)
# ══════════════════════════════════════════════════════════════════════════════

class TestValidation:
    def test_missing_gross_tonnage(self, client):
        """Missing gross_tonnage returns 422 with clear error."""
        payload = {
            "vessel_metadata": {"name": "Test"},
            "technical_specs": {
                "type": "Bulk Carrier",
                "loa_meters": 200.0,
            },
            "operational_data": {
                "port_id": "durban",
                "days_alongside": 3.0,
                "arrival_time": "2024-11-15T10:00:00",
                "departure_time": "2024-11-18T10:00:00",
                "activity": "Cargo",
            },
        }
        r = client.post("/api/v1/calculate", json=payload)
        assert r.status_code == 422
        data = r.json()
        assert "detail" in data
        # Should mention gross_tonnage
        detail_str = str(data["detail"]).lower()
        assert "gross_tonnage" in detail_str

    def test_missing_loa(self, client):
        """Missing loa_meters returns 422."""
        payload = {
            "vessel_metadata": {"name": "Test"},
            "technical_specs": {
                "type": "Bulk Carrier",
                "gross_tonnage": 51300.0,
            },
            "operational_data": {
                "port_id": "durban",
                "days_alongside": 3.0,
                "arrival_time": "2024-11-15T10:00:00",
                "departure_time": "2024-11-18T10:00:00",
                "activity": "Cargo",
            },
        }
        r = client.post("/api/v1/calculate", json=payload)
        assert r.status_code == 422
        detail_str = str(r.json()["detail"]).lower()
        assert "loa_meters" in detail_str

    def test_invalid_json(self, client):
        """Non-JSON body returns 422."""
        r = client.post(
            "/api/v1/calculate",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 422

    def test_empty_body(self, client):
        """Empty body returns 422."""
        r = client.post(
            "/api/v1/calculate",
            json={},
        )
        assert r.status_code == 422


# ══════════════════════════════════════════════════════════════════════════════
# Citation Service
# ══════════════════════════════════════════════════════════════════════════════

class TestCitation:
    def test_citation_light_dues(self, client):
        """Citation lookup for 'Light Dues' returns page and section."""
        r = client.get("/api/v1/citation/Light Dues")
        assert r.status_code == 200
        data = r.json()
        assert data["found"] is True
        assert data["citation"]["page"] > 0
        assert len(data["citation"]["section"]) > 0

    def test_citation_port_dues(self, client):
        """Citation lookup for 'Port Dues' returns page and section."""
        r = client.get("/api/v1/citation/Port Dues")
        assert r.status_code == 200
        data = r.json()
        assert data["found"] is True

    def test_citation_not_found(self, client):
        """Unknown charge returns found=False."""
        r = client.get("/api/v1/citation/Nonexistent Charge")
        assert r.status_code == 200
        data = r.json()
        assert data["found"] is False
        assert data["citation"] is None

    def test_list_citations(self, client):
        """GET /citations returns a dict of all available citations."""
        r = client.get("/api/v1/citations")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, dict)
        # Should have at least the standard 6 charge types
        assert len(data) >= 6
        # Each citation should have page and section
        for name, cit in data.items():
            assert "page" in cit
            assert "section" in cit

    def test_citation_page_extract_not_found(self, client):
        """PDF page extract for unknown charge returns 404."""
        r = client.get("/api/v1/citation/Nonexistent Charge/page")
        assert r.status_code == 404


# ══════════════════════════════════════════════════════════════════════════════
# COMBINED FLOW
# ══════════════════════════════════════════════════════════════════════════════

class TestEndToEnd:
    def test_calculate_then_audit_then_citation(self, client, sudestada_payload):
        """
        Full flow: calculate -> verify audit -> verify citations exist for each charge.
        """
        # Step 1: Calculate
        r = client.post("/api/v1/calculate", json=sudestada_payload)
        assert r.status_code == 200
        calc = r.json()
        audit_id = calc["audit_id"]

        # Step 2: Retrieve audit
        r2 = client.get(f"/api/v1/audit/{audit_id}")
        assert r2.status_code == 200
        audit = r2.json()
        assert audit["vessel_name"] == "SUDESTADA"
        assert len(audit["output_data"]) == 6

        # Step 3: Verify citation exists for each breakdown charge
        for item in calc["breakdown"]:
            charge_name = item["charge"]
            r3 = client.get(f"/api/v1/citation/{charge_name}")
            assert r3.status_code == 200
            cit_data = r3.json()
            assert cit_data["found"] is True, f"Citation not found for {charge_name}"
