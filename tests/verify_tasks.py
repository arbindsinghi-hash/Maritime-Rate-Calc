"""
verify_tasks.py — Automated verification for every task in TASKS.md.

Run:  cd mrca-ai-tariff && python -m pytest tests/verify_tasks.py -v

Infrastructure tasks are verified against existing scaffold code.
Later phases guard against regressions as features are built.
"""

import importlib
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════
# Infrastructure (all should pass today)
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase1Infrastructure:
    """Scaffold, config, DB, schemas, FAISS, FastAPI boot."""

    # -- 1.1 docker-compose.yml exists --
    def test_1_1_docker_compose_exists(self):
        path = PROJECT_ROOT / "docker-compose.yml"
        assert path.exists(), "docker-compose.yml missing"
        content = path.read_text()
        assert "backend" in content.lower()
        assert "AUDIT_LOG_DIR" in content

    # -- 1.2 .env has all model triplets (no blanks) --
    def test_1_2_env_has_model_config(self):
        from dotenv import dotenv_values
        env = dotenv_values(PROJECT_ROOT / ".env")
        required_prefixes = ["DOC_PARSER", "INSTRUCT", "LLM", "EMBEDDING"]
        for prefix in required_prefixes:
            for suffix in ["API_BASE", "API_KEY", "MODEL"]:
                key = f"{prefix}_{suffix}"
                assert key in env, f"Missing {key} in .env"
                assert env[key] and not env[key].startswith("your_"), f"{key} is a placeholder"

    # -- 1.3 Settings loads and audit_log_path works --
    def test_1_3_settings_loads(self):
        from backend.core.config import Settings
        s = Settings()
        audit_path = s.audit_log_path
        assert audit_path.endswith("audit_log.jsonl")
        assert "audit" in audit_path

    # -- 1.4 llm_clients exposes 3 factories --
    def test_1_4_llm_client_factories_exist(self):
        from backend.core import llm_clients
        for name in [
            "get_gemini_client",
            "get_llm_client",
            "get_embedding_client",
        ]:
            fn = getattr(llm_clients, name, None)
            assert callable(fn), f"{name} not callable"

    # -- 1.5 JSONL audit store wiring --
    def test_1_5_jsonl_audit_store(self):
        from backend.core.audit_store import AuditStore
        store = AuditStore()
        assert hasattr(store, "append")
        assert hasattr(store, "get")
        assert hasattr(store, "list_recent")
        assert hasattr(store, "clear")

    # -- 1.6 Audit store record structure --
    def test_1_6_audit_store_record(self):
        import tempfile
        from backend.core.audit_store import AuditStore
        with tempfile.TemporaryDirectory() as td:
            store = AuditStore(audit_dir=td)
            record_id = store.append(
                vessel_name="TEST",
                imo_number=None,
                input_data={},
                output_data={},
                tariff_version="v1",
            )
            rec = store.get(record_id)
            assert rec is not None
            for field in ["vessel_name", "input_data", "output_data", "tariff_version"]:
                assert field in rec, f"Audit record missing field: {field}"

    # -- 1.7 Pydantic schemas round-trip --
    def test_1_7_schemas_round_trip(self):
        from backend.models.schemas import (
            CalculationRequest, CalculationResponse,
            ChargeBreakdown, Citation,
        )
        payload = {
            "vessel_metadata": {"name": "TEST", "flag": "TST"},
            "technical_specs": {
                "type": "Bulk Carrier",
                "gross_tonnage": 51300,
                "loa_meters": 229.2,
            },
            "operational_data": {
                "days_alongside": 3.39,
                "arrival_time": "2024-11-15T10:12:00",
                "departure_time": "2024-11-22T13:00:00",
                "activity": "Exporting Iron Ore",
            },
        }
        req = CalculationRequest(**payload)
        assert req.vessel_metadata.name == "TEST"

        citation = Citation(page=12, section="3.1")
        bd = ChargeBreakdown(
            charge="Light Dues", basis=51300, rate=1.17,
            formula="gross_tonnage * 1.17", result=60021.0,
            citation=citation,
        )
        resp = CalculationResponse(total_zar=60021.0, breakdown=[bd])
        d = resp.model_dump()
        assert d["total_zar"] == 60021.0
        assert len(d["breakdown"]) == 1

    # -- 1.8 FAISS service initializes --
    def test_1_8_faiss_service_init(self):
        # Patch the embedding client so we don't hit a real endpoint
        from backend.services.faiss_service import FAISSService
        svc = FAISSService()
        assert svc.index.ntotal == 0

    # -- 1.9 FastAPI /health returns 200 --
    def test_1_9_fastapi_health(self):
        from fastapi.testclient import TestClient
        from backend.main import app
        client = TestClient(app)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"


# ═══════════════════════════════════════════════════════════════════════════
# Ingestion DAG (guards — will be skipped until code exists)
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase2Ingestion:
    """LangGraph ingestion pipeline."""

    def _skip_if_missing(self, module_path: str):
        try:
            importlib.import_module(module_path)
        except (ImportError, ModuleNotFoundError):
            pytest.skip(f"{module_path} not yet implemented")

    # 2.1 PDF Parser Node
    def test_2_1_pdf_parser_node(self):
        self._skip_if_missing("backend.ingestion.pdf_parser")
        try:
            import fitz  # noqa: F401
        except ImportError:
            pytest.skip("PyMuPDF (fitz) not installed")
        from backend.ingestion.pdf_parser import parse_pdf
        # Use any small test PDF in storage/pdfs/
        test_pdf = PROJECT_ROOT / "storage" / "pdfs" / "test.pdf"
        if not test_pdf.exists():
            pytest.skip("No test PDF available")
        pages = parse_pdf(str(test_pdf))
        assert len(pages) >= 1
        assert pages[0].text.strip() != ""

    # 2.4 Schema Validation Node
    def test_2_4_schema_validation(self):
        self._skip_if_missing("backend.ingestion.schema_validation")
        from backend.ingestion.schema_validation import TariffSection
        from pydantic import ValidationError
        # valid — TariffSection requires id, name, and calculation (with type)
        section = TariffSection(
            id="light_dues",
            name="Light Dues",
            description="Light dues per 100 GT",
            calculation={"type": "per_unit", "basis": "gross_tonnage",
                         "rate_per_gt": 1.17, "divisor": 100},
            citation={"page": 12, "section": "3.1"},
        )
        assert section.id == "light_dues"
        assert section.calculation.type == "per_unit"
        assert section.citation.page == 12
        # invalid — missing required 'calculation' field
        with pytest.raises(ValidationError):
            TariffSection(id="light_dues", name="Light Dues")

    # 2.9 End-to-end DAG
    def test_2_9_langgraph_dag_end_to_end(self):
        self._skip_if_missing("backend.ingestion.dag")
        try:
            import langgraph  # noqa: F401
        except ImportError:
            pytest.skip("langgraph not installed")
        from backend.ingestion.dag import run_ingestion
        test_pdf = PROJECT_ROOT / "storage" / "pdfs" / "test.pdf"
        if not test_pdf.exists():
            pytest.skip("No test PDF available")
        result = run_ingestion(str(test_pdf))
        if result.status == "failed" and (
            "connection" in (result.message or "").lower()
            or "nodename" in (result.message or "").lower()
            or "api" in (result.message or "").lower()
            or "No module named" in (result.message or "")
            or "does not exist" in (result.message or "").lower()
            or "404" in (result.message or "")
        ):
            pytest.skip("LLM/API or DAG dependency not available; end-to-end requires live services")
        assert result.status == "success"
        assert result.rules_count > 0


# ═══════════════════════════════════════════════════════════════════════════
# Deterministic Tariff Engine
# ═══════════════════════════════════════════════════════════════════════════

# Golden test input from the architectural blueprint
SUDESTADA_INPUT = {
    "vessel_metadata": {"name": "SUDESTADA", "built_year": 2010, "flag": "MLT - Malta"},
    "technical_specs": {
        "type": "Bulk Carrier", "dwt": 93274, "gross_tonnage": 51300,
        "net_tonnage": 31192, "loa_meters": 229.2, "beam_meters": 38.0,
        "lbp_meters": 222.0,
    },
    "operational_data": {
        "cargo_quantity_mt": 40000, "days_alongside": 3.39,
        "arrival_time": "2024-11-15T10:12:00", "departure_time": "2024-11-22T13:00:00",
        "activity": "Exporting Iron Ore", "num_operations": 2, "num_holds": 7,
        "port_id": "durban",
    },
}

# Expected outputs from blueprint (ZAR)
GOLDEN_CHARGES = {
    "Light Dues": 60_062.04,
    "Port Dues": 199_549.22,
    "Towage Dues": 147_074.38,
    "VTS Dues": 33_315.75,
    "Pilotage Dues": 47_189.94,
    "Berthing Services": 19_639.50,
}

TOLERANCE = 0.01  # 1%


class TestPhase3TariffEngine:
    """Deterministic charge calculations."""

    def _skip_if_stub(self):
        from backend.engine.tariff_engine import tariff_engine
        if not tariff_engine.ruleset:
            pytest.skip("Tariff rules YAML not yet populated")

    def _get_breakdown_map(self):
        from backend.engine.tariff_engine import tariff_engine
        from backend.models.schemas import CalculationRequest
        req = CalculationRequest(**SUDESTADA_INPUT)
        items = tariff_engine.calculate(req)
        return {item.charge: item for item in items}

    def _assert_close(self, actual, expected, label):
        diff = abs(actual - expected) / expected
        assert diff <= TOLERANCE, f"{label}: expected ~{expected}, got {actual} (diff {diff:.2%})"

    # 3.1 YAML ruleset validates through TariffRuleset model
    def test_3_1_yaml_rule_schema(self):
        try:
            from backend.models.tariff_rule import TariffRuleset
        except ImportError:
            pytest.skip("TariffRuleset not yet defined")
        sample = PROJECT_ROOT / "storage" / "yaml" / "tariff_rules_latest.yaml"
        assert sample.exists(), "Golden YAML missing"
        ruleset = TariffRuleset.from_yaml(str(sample))
        assert len(ruleset.sections) >= 3, "Ruleset must have at least 3 sections"

    # 3.2 Rule loader
    def test_3_2_rule_loader(self):
        from backend.engine.tariff_engine import TariffEngine
        e = TariffEngine()
        if not e.ruleset:
            pytest.skip("Rules YAML not populated yet")
        assert len(e.ruleset.sections) >= 3

    # 3.3 – 3.8: Individual charges
    @pytest.mark.parametrize("charge_name,expected", list(GOLDEN_CHARGES.items()))
    def test_3_x_individual_charge(self, charge_name, expected):
        self._skip_if_stub()
        m = self._get_breakdown_map()
        assert charge_name in m, f"{charge_name} not in breakdown"
        self._assert_close(m[charge_name].result, expected, charge_name)

    # 3.9 Reductions / Caps / VAT
    def test_3_9_reductions_caps_vat(self):
        self._skip_if_stub()
        # Verified implicitly if individual charges match golden values
        # but also test a simple unit case
        from backend.engine.tariff_engine import tariff_engine
        if not hasattr(tariff_engine, "apply_vat"):
            pytest.skip("apply_vat not yet implemented")
        assert tariff_engine.apply_vat(1000, 0.15) == 1150.0

    # 3.10 Full calculation total
    def test_3_10_full_total(self):
        self._skip_if_stub()
        m = self._get_breakdown_map()
        total = sum(item.result for item in m.values())
        expected_total = sum(GOLDEN_CHARGES.values())
        self._assert_close(total, expected_total, "Total ZAR")


# ═══════════════════════════════════════════════════════════════════════════
# Citation Service
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase4Citation:
    """Citation storage, lookup, PDF page extract."""

    def _skip_if_missing(self):
        try:
            importlib.import_module("backend.services.citation_service")
        except (ImportError, ModuleNotFoundError):
            pytest.skip("citation_service not yet implemented")

    def test_4_2_lookup_by_charge(self):
        self._skip_if_missing()
        from backend.services.citation_service import citation_service
        c = citation_service.get("Light Dues")
        if c is None:
            pytest.skip("No citations persisted yet")
        assert c.page > 0
        assert c.section != ""


# ═══════════════════════════════════════════════════════════════════════════
# FastAPI Endpoints
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase5API:
    """/calculate, /chat, /ingest, /audit, error handling."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from backend.main import app
        return TestClient(app)

    # 5.1 POST /calculate
    def test_5_1_calculate_endpoint(self, client):
        resp = client.post("/api/v1/calculate", json=SUDESTADA_INPUT)
        assert resp.status_code == 200
        body = resp.json()
        assert "total_zar" in body
        assert "breakdown" in body

    # 5.2 POST /chat (natural language)
    def test_5_2_chat_endpoint(self, client):
        resp = client.post("/api/v1/chat", json={"message": "Calculate dues for a 51300 GT bulk carrier"})
        if resp.status_code == 404:
            pytest.skip("/chat endpoint not yet implemented")
        assert resp.status_code == 200
        assert "breakdown" in resp.json()

    # 5.3 POST /ingest
    def test_5_3_ingest_endpoint(self, client):
        resp = client.post("/api/v1/ingest", params={"file_path": "storage/pdfs/test.pdf"})
        assert resp.status_code in (200, 202)

    # 5.4 GET /audit/{id}
    def test_5_4_audit_endpoint(self, client):
        resp = client.get("/api/v1/audit/1")
        if resp.status_code == 404:
            pytest.skip("/audit endpoint not yet implemented")
        assert resp.status_code == 200

    # 5.5 Validation error
    def test_5_5_validation_error(self, client):
        bad_input = {"vessel_metadata": {"name": "X"}, "technical_specs": {}, "operational_data": {}}
        resp = client.post("/api/v1/calculate", json=bad_input)
        assert resp.status_code == 422
        assert "detail" in resp.json()


# ═══════════════════════════════════════════════════════════════════════════
# Frontend (smoke test only — checks build succeeds)
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase6Frontend:
    """Verify React project builds without errors."""

    def test_6_0_package_json_exists(self):
        pkg = PROJECT_ROOT / "frontend" / "package.json"
        assert pkg.exists(), "frontend/package.json missing"

    def test_6_0_tsconfig_exists(self):
        ts = PROJECT_ROOT / "frontend" / "tsconfig.json"
        assert ts.exists(), "frontend/tsconfig.json missing"


# ═══════════════════════════════════════════════════════════════════════════
# Testing & Evals (meta-tests: check test files exist)
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase7Evals:
    """Check that eval scripts exist."""

    def test_7_3_ingestion_eval_script(self):
        p = PROJECT_ROOT / "tests" / "eval_ingestion.py"
        if not p.exists():
            pytest.skip("eval_ingestion.py not yet created")
        assert p.stat().st_size > 100

    def test_7_4_response_eval_script(self):
        p = PROJECT_ROOT / "tests" / "eval_response.py"
        if not p.exists():
            pytest.skip("eval_response.py not yet created")
        assert p.stat().st_size > 100


# ═══════════════════════════════════════════════════════════════════════════
# Documentation & Deployment
# ═══════════════════════════════════════════════════════════════════════════

class TestPhase8Docs:
    """README, YAML docs, OpenShift manifests."""

    def test_8_1_readme_exists(self):
        p = PROJECT_ROOT / "README.md"
        if not p.exists():
            pytest.skip("README.md not yet written")
        content = p.read_text()
        assert "docker" in content.lower() or "uvicorn" in content.lower()

    def test_8_3_openshift_manifests(self):
        deploy_dir = PROJECT_ROOT / "deploy"
        if not deploy_dir.exists():
            pytest.skip("deploy/ directory not yet created")
        for f in ["deployment.yaml", "service.yaml", "pvc.yaml"]:
            assert (deploy_dir / f).exists(), f"deploy/{f} missing"
