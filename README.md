# Marc · Port Tariff Calculator

**Deterministic computation of port dues with auditable citations and LLM-assisted Document Q&A.**

This repository provides a full-stack application to calculate maritime port tariffs (e.g. Transnet NPA) from structured vessel and visit data. It includes a **deterministic tariff engine** (YAML rules), **audit trail** (JSONL), **citation service** (PDF page/section), and optional **natural-language chat** (Gemini) for “calculate dues for a 51300 GT bulk carrier at Durban”–style queries.

---

## Table of contents

- [Features](#features)
- [Architecture](#architecture)
- [Repository structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Local setup](#local-setup)
- [Running the application](#running-the-application)
- [How to use](#how-to-use)
- [API reference](#api-reference)
- [Configuration](#configuration)
- [Testing](#testing)
- [Contributing](#contributing)

---

## Features

- **Structured form** — Submit vessel (name, type, GT, LOA) and visit (port, days alongside, arrival/departure, activity) to get a line-item breakdown.
- **Document Q&A** — Ask in plain language (e.g. “Calculate dues for a 51300 GT bulk carrier at Durban for 3 days”); the backend extracts fields and runs the same engine.
- **Breakdown table** — Charge, basis, rate, formula, result per line; total with VAT.
- **PDF viewer** — View the tariff PDF and jump to the page for a selected charge (citation).
- **Audit trail** — Expandable panel with full audit JSON for the last (or selected) calculation; supports both form and chat flows.
- **Ingestion pipeline** — LangGraph DAG to turn PDFs into YAML rules (parser → Gemini extract → fusion → section chunker → vector indexer → clause mapping → schema validation → eval → LLM reviewer → persist).

---

## Architecture

### Design philosophy

- **Deterministic computation** — The tariff engine never depends on an LLM at runtime; all charges are computed from YAML rules.
- **Auditability** — Every line item is tied to a source PDF page and section (citation).
- **Extensibility** — YAML-driven tariff rules support versioning and portability across ports.
- **Separation of concerns** — Runtime engine is separate from offline ingestion; ingestion runs once to produce/update rules.
- **Simplicity** — One primary model (Gemini 2.5 Pro) and one ingestion path; no multi-model orchestration.

### System architecture

**Offline ingestion (one-time, LangGraph DAG):** PDF → structured YAML + JSONL audit + FAISS index. Optional; used to (re)generate or validate tariff rules from a PDF.

```
┌────────────────────────────────────────────────────────────────────────┐
│                    OFFLINE INGESTION (one-time, LangGraph DAG)         │
│  PDF ──► PyMuPDF ──► Gemini 2.5 Pro ──► Page Fusion ──► Section        │
│          (text +      (image + OCR       (merge +        Chunker       │
│           bbox)        → Markdown)        validate)      (split by     │
│                                                           section)     │
│                                                              │         │
│  REPAIR LOOP (max 3 retries): Clause Mapping ──► Schema Validation     │
│  ──► Ingestion Eval (vs golden) ──► LLM Reviewer (confidence < 0.8     │
│  → re-run mapping) ──► Persist (GOLDEN YAML + JSONL + FAISS)           │
└────────────────────────────────────────────────────────────────────────┘
```

**Runtime (per request):** Deterministic engine only; no LLM in the calculation path.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       RUNTIME (per request)                              │
│  Vessel Input ──► Tariff Engine  ──►  Breakdown + Citations              │
│  (JSON)           (deterministic,     (line items, totals,               │
│                    YAML-driven)        audit log)                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### Modular components

1. **Frontend (Next.js)** — Vessel form, free-text chat, line-item breakdown, PDF viewer with citation highlight, audit trail.
2. **API (FastAPI)** — REST endpoints for calculate, chat, audit, citation, tariff PDF, and (optional) ingest.
3. **Offline ingestion (LangGraph)** — 10-node DAG with conditional repair loop: PDF → Gemini 2.5 Pro → YAML tariff sections.
4. **Tariff engine (Python)** — Loads YAML, dispatches by charge type, applies reductions/surcharges/VAT; fully deterministic.
5. **Storage** — YAML (rules), JSONL (`storage/audit/audit_log.jsonl`), FAISS (semantic retrieval), and PDFs in persistent storage.

### Offline ingestion workflow (10-node DAG)

Purpose: convert PDFs into structured YAML tariff sections (e.g. 12-field `TariffSection` schema).

```
PDF Parser ──► Gemini Extract ──► Page Fusion ──► Section Chunker ──► Vector Indexer
                                                                           │
                                                                           ▼
                ┌─── REPAIR LOOP (up to 3 retries if confidence < 0.8) ───┐
                │                                                          │
                │  Clause Mapping ──► Schema Validation ──► Ingestion Eval │
                │  (Gemini → YAML)     (Pydantic)           (vs golden)    │
                │        ▲                                       │         │
                │        └──────── LLM Reviewer ◄────────────────┘         │
                │                  (confidence ≥ 0.8 → exit loop)          │
                └──────────────────────────────────────────────────────────┘
                                             │
                                             ▼
                                       Persist Rules
                                    (YAML + JSONL + FAISS)
```

| Node                        | Role                                                                                         |
| --------------------------- | -------------------------------------------------------------------------------------------- |
| **PDF Parser**        | PyMuPDF: per-page text and bounding boxes; output cached.                                    |
| **Gemini Extract** (`table_extract`)   | Page as base64 PNG + OCR text → Gemini 2.5 Pro → structured Markdown (headers, tables).    |
| **Page Fusion**       | Merge Gemini + PDF text; Gemini primary; recover unmatched PDF lines; confidence scoring.    |
| **Section Chunker**   | Split fused content by section headers (e.g. "1.1 LIGHT DUES"); preamble as section `"0"`. |
| **Vector Indexer**    | Embed section chunks into FAISS (best-effort; continues if embedding unavailable).           |
| **Clause Mapping**    | Per-section call to Gemini 2.5 Pro → draft 12-field YAML sections.                          |
| **Schema Validation** | Validate drafts against Pydantic `TariffSection`; reject or coerce invalid fields.         |
| **Ingestion Eval**    | Compare extracted sections to golden YAML; precision/recall/F1.                              |
| **LLM Reviewer**      | Gemini 2.5 Pro review; confidence < 0.8 and retries < 3 → loop back to Clause Mapping.      |
| **Persist Rules**     | Write the Golden YAML to `YAML_DIR`, append citations to JSONL, index in FAISS.            |

**Runtime flow (calculation):**

1. User submits via **Structured Form** or **Document Q&A** (chat uses Gemini only for field extraction, not for amounts).
2. **Tariff Engine** loads `storage/yaml/tariff_rules_*.yaml`, dispatches by charge type, applies reductions/surcharges/VAT.
3. Result is written to the **audit store** (JSONL) and returned with `breakdown` and `audit_id`.
4. Frontend shows the **breakdown table** and **audit trail**; citations link to the tariff PDF.

---

## Repository structure

```
mrca-ai-tariff/
├── backend/                 # FastAPI app, engine, ingestion
│   ├── api/                 # REST endpoints (endpoints.py)
│   ├── core/                # Config, audit store, LLM clients, FAISS
│   ├── engine/              # Tariff engine, handlers, condition evaluator
│   ├── ingestion/           # PDF parser, Gemini extract, page fusion, DAG
│   ├── models/              # Pydantic schemas, tariff rule models
│   └── services/            # Citation service, FAISS
├── frontend/                # Next.js 16 app (React 19, Tailwind CSS v4, shadcn/ui)
│   ├── src/
│   │   ├── app/             # App router, page
│   │   ├── components/      # StructuredForm, ChatMode, BreakdownTable, PdfViewer, AuditPanel, PromptsPanel
│   │   └── lib/             # API client, types
│   └── public/
├── evals/                   # Ingestion evaluation (ingestion_eval.py)
├── scripts/                 # CLI scripts (run_dag_e2e.py, run_extract.py, etc.)
├── storage/                 # Runtime data (created on first run)
│   ├── audit/               # audit_log.jsonl
│   ├── faiss/               # FAISS vector index (best-effort)
│   ├── pdfs/                # Tariff PDFs
│   └── yaml/                # tariff_rules_*.yaml
├── pipeline/                # YAML pipeline config and runner
├── tests/                   # Pytest (test_engine, test_api, verify_tasks)
├── configs/                 # Additional configuration files
├── output/                  # Cached intermediate outputs (gitignored)
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── .env.example
```

---

## Prerequisites

- **Docker & Docker Compose** (for containerised run), or
- **Python 3.11+** and **Node.js 20+** (for local dev)
- For **Document Q&A**: a **Gemini API key** (see [Configuration](#configuration))

---

## Local setup

### 1. Clone the repository

```bash
git clone https://github.com/arbindsinghi-hash/Maritime-Rate-Calc.git
cd Maritime-Rate-Calc
```

### 2. Environment variables

Copy the example env and set at least the required keys:

```bash
cp .env.example .env
# Edit .env: set GEMINI_API_KEY for chat; optional LLM/EMBEDDING for ingestion
```

**Do not commit `.env`** — it is listed in `.gitignore`. Before pushing, run `git status` and ensure `.env` (and any `*.pem` / `*.key` files) are not staged.

Required for **calculation only** (no chat):

- `STORAGE_DIR`, `YAML_DIR`, `AUDIT_LOG_DIR` — can stay as defaults if using Docker or local `storage/`.

Required for **Document Q&A (chat)**:

- `GEMINI_API_KEY` — Required. Used by Gemini 2.5 Pro (ingestion) and Gemini 2.5 Flash (chat NL extraction).
- `GEMINI_API_BASE`, `GEMINI_CHAT_MODEL` — optional overrides (defaults in `.env.example`).

See [Configuration](#configuration) for full list.

### 3. Tariff rules and PDF

Ensure the engine has YAML rules and (optional) a PDF for citations:

- Place `tariff_rules_latest.yaml` (or your version) under `storage/yaml/`.
- Place the tariff PDF (e.g. `Port Tariff.pdf`) under `storage/pdfs/`.

If you don’t have them yet, the app may still start but `/health` can report rules not loaded; calculation will return 503 until YAML is present.

### 4. Install dependencies (local dev, no Docker)

**Backend:**

```bash
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

**Frontend:**

```bash
cd frontend && npm install && cd ..
```

---

## Running the application

### Option A: Docker Compose (recommended for first run)

```bash
# Build and start backend + frontend (ports 8000, 3000)
make up
# or: docker compose --profile app up -d

# Open in browser: http://localhost:3000
# API docs: http://localhost:8000/docs
```

Stop:

```bash
make down
```

### Option B: Local development (backend + frontend in parallel)

```bash
# Backend on :8000, frontend on :3000 (with hot reload)
make dev
```

Or run separately:

```bash
make dev-backend   # Terminal 1: uvicorn on :8000
make dev-frontend  # Terminal 2: Next.js on :3000
```

Frontend proxies `/api` to the backend; set `NEXT_PUBLIC_API_URL` if the API is on another host/port.

### Option C: Backend only (e.g. for API integration)

```bash
make dev-backend
# API: http://localhost:8000
# Docs: http://localhost:8000/docs
```

---

## How to use

### In the UI

1. **Structured Form** — Fill vessel (name, type, gross tonnage, LOA) and visit (port, days alongside, arrival/departure, activity). Click **Calculate**. The breakdown table and total (with VAT) appear; open **Audit trail** to see the stored request/response.
2. **Document Q&A** — Type a question such as “Calculate dues for a 51300 GT bulk carrier at Durban for 3 days” and send. The backend extracts fields, runs the engine, and shows the same breakdown and audit (requires `GEMINI_API_KEY`).
3. **Breakdown table** — Click a row to jump the PDF viewer to the cited page for that charge.
4. **Audit trail** — Use the dropdown to select an audit by ID or by question (for chat); the panel shows full JSON.

### With the API (curl)

**Health:**

```bash
curl -s http://localhost:8000/health
```

**Calculate (structured payload):**

```bash
curl -s -X POST http://localhost:8000/api/v1/calculate \
  -H "Content-Type: application/json" \
  -d '{
    "vessel_metadata": {"name": "SUDESTADA", "flag": "MLT"},
    "technical_specs": {
      "type": "Bulk Carrier",
      "gross_tonnage": 51300,
      "loa_meters": 229.2
    },
    "operational_data": {
      "port_id": "durban",
      "days_alongside": 3.39,
      "arrival_time": "2024-11-15T10:12:00",
      "departure_time": "2024-11-22T13:00:00",
      "activity": "Exporting Iron Ore"
    }
  }'
```

**Chat (natural language):**

```bash
curl -s -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Calculate dues for a 51300 GT bulk carrier at Durban for 3 days"}'
```

**Get audit by ID:**

```bash
curl -s http://localhost:8000/api/v1/audit/1
```

---

## API reference

Base URL: `http://localhost:8000` (or your backend host). All API routes are under **`/api/v1`**.

| Method   | Path                                    | Description                                                                                                                                                                                                                  |
| -------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET`  | `/`                                   | Service info and link to `/docs`.                                                                                                                                                                                          |
| `GET`  | `/health`                             | Health check. Returns `200` with `status: "healthy"` when tariff rules are loaded; `503` with `status: "unhealthy"` otherwise.                                                                                       |
| `GET`  | `/ready`                              | Readiness: YAML loaded, audit dir writable, optional Gemini key.                                                                                                                                                             |
| `POST` | `/api/v1/calculate`                   | Run tariff calculation.**Body:** `CalculationRequest` (JSON). **Response:** `CalculationResponse` with `total_zar`, `vat_amount`, `total_with_vat`, `breakdown[]`, `audit_id`, `tariff_version`. |
| `POST` | `/api/v1/chat`                        | Natural-language calculation.**Body:** `{ "message": "…", "api_key": "…" }` (api_key optional if server has `GEMINI_API_KEY`). **Response:** Same shape as `/calculate` plus `extracted_fields`.       |
| `GET`  | `/api/v1/chat/status`                 | Whether chat is available (Gemini configured).                                                                                                                                                                               |
| `GET`  | `/api/v1/config`                      | Form config (ports, vessel types, purposes) from rules.                                                                                                                                                                      |
| `GET`  | `/api/v1/audit`                       | List recent audit summaries (id, vessel_name, timestamp, user_message). Query:`limit` (default 50).                                                                                                                        |
| `GET`  | `/api/v1/audit/{audit_id}`            | Full audit entry (id, vessel_name, input_data, output_data, tariff_version).                                                                                                                                                 |
| `GET`  | `/api/v1/citation/{charge_name}`      | Citation for a charge (page, section).                                                                                                                                                                                       |
| `GET`  | `/api/v1/citation/{charge_name}/page` | PDF bytes for the cited page. Query:`pdf_filename`.                                                                                                                                                                        |
| `GET`  | `/api/v1/citations`                   | List all citations.                                                                                                                                                                                                          |
| `GET`  | `/api/v1/tariff-pdf`                  | Full tariff PDF. Query:`filename` (default `Port Tariff.pdf`).                                                                                                                                                           |
| `POST` | `/api/v1/ingest`                      | Trigger ingestion (PDF → YAML). Query:`file_path` or body: multipart file. Returns `job_id` / status (stub if DAG unavailable).                                                                                         |
| `GET`  | `/api/v1/prompts/config`              | Whether the developer prompt panel is enabled (`ENABLE_PROMPT_PANEL`).                                                                                                                                                   |
| `GET`  | `/api/v1/prompts`                     | List recorded LLM prompt interactions (user query, system prompt, raw response, parsed result/error). Only available when prompt panel is enabled.                                                                        |

**CalculationRequest** (main fields):

- `vessel_metadata`: `name` (required), `flag`, etc.
- `technical_specs`: `type`, `gross_tonnage`, `loa_meters` (required), `imo_number`, `dwt`, …
- `operational_data`: `port_id`, `days_alongside`, `arrival_time`, `departure_time`, `activity` (required), `purpose`, `num_operations`, `num_holds`, `num_tug_operations`, `cargo_quantity_mt`, …

**CalculationResponse**:

- `total_zar`, `vat_amount`, `total_with_vat`, `currency`, `tariff_version`, `audit_id`, `breakdown[]` (each: `charge`, `basis`, `rate`, `formula`, `result`, `citation`).

Interactive API docs: **http://localhost:8000/docs**.

---

## Configuration

| Variable                | Description                                          | Default                                                      |
| ----------------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| `STORAGE_DIR`         | Base storage path                                    | `./storage`                                                |
| `YAML_DIR`            | Tariff YAML directory                                | `./storage/yaml`                                           |
| `AUDIT_LOG_DIR`       | Audit JSONL directory                                | `./storage/audit`                                          |
| `PDF_DIR`             | Tariff PDFs                                          | `./storage/pdfs`                                           |
| `FAISS_INDEX_DIR`     | FAISS vector index directory                         | `./storage/faiss`                                          |
| `GEMINI_API_KEY`      | Gemini API key (ingestion + chat)                    | —                                                           |
| `GEMINI_API_BASE`     | Gemini API base URL                                  | `https://generativelanguage.googleapis.com/v1beta/openai/` |
| `GEMINI_MODEL`        | Model for ingestion (VL extraction, clause mapping)  | `gemini-2.5-pro`                                           |
| `GEMINI_CHAT_MODEL`   | Model for chat NL extraction                         | `gemini-2.5-flash`                                         |
| `GEMINI_TIMEOUT`      | Per-page timeout for Gemini calls (seconds)          | `120`                                                      |
| `LLM_API_BASE`        | LLM API base URL (optional, for gpt-oss)             | —                                                           |
| `LLM_API_KEY`         | LLM API key (optional)                               | —                                                           |
| `LLM_MODEL`           | LLM model (optional)                                 | `openai/gpt-oss-120b`                                     |
| `LLM_TIMEOUT`         | LLM request timeout (seconds)                        | `300`                                                      |
| `LLM_TEMPERATURE`     | LLM temperature                                      | `0.2`                                                      |
| `LLM_TOP_P`           | LLM top-p                                            | `0.9`                                                      |
| `EMBEDDING_API_BASE`  | Embedding endpoint base URL                          | —                                                           |
| `EMBEDDING_API_KEY`   | Embedding API key                                    | —                                                           |
| `EMBEDDING_MODEL`     | Embedding model                                      | `nvidia/llama-3.2-nv-embedqa-1b-v2`                       |
| `CORS_ORIGINS`        | Allowed origins (comma-separated)                    | `*`                                                        |
| `ENABLE_PROMPT_PANEL` | Developer prompt panel in UI                         | `false`                                                    |

See `.env.example` for the full set (including optional LLM/EMBEDDING for ingestion).

---

## Testing

```bash
# All engine + API tests
make test

# Engine unit tests only
make test-engine

# API integration tests only
make test-api

# Run tests inside Docker
make test-docker
```

---

## Contributing

We welcome community contributions. Please open an issue for bugs or feature ideas, and a pull request for code changes. Ensure tests pass and follow the existing code style (e.g. `make lint`).
