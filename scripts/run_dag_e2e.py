#!/usr/bin/env python3
"""
End-to-end LangGraph DAG run on the first N pages of the Port Tariff PDF.

Usage:
    # Full fresh run (calls Gemini for all nodes):
    python scripts/run_dag_e2e.py --pages 6

    # Resume from a specific node (reuses cached JSON from output/dag_e2e/):
    python scripts/run_dag_e2e.py --pages 6 --resume section_chunker

    # Resume from clause_mapping (skips pdf_parser → page_fusion, saves ~200s):
    python scripts/run_dag_e2e.py --pages 6 --resume clause_mapping

Steps:
  1. Extract first N pages from Port Tariff.pdf → temp PDF
  2. Run the full ingestion DAG:
       pdf_parser → table_extract → page_fusion → section_chunker →
       vector_indexer → clause_mapping → schema_validation →
       ingestion_eval → llm_reviewer → (repair loop) → persist
  3. Print detailed per-node output, section details, and final result
  4. Save all artefacts to output/dag_e2e/
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

import fitz  # PyMuPDF

# Ordered list of DAG nodes — must match the order in dag.py
NODE_ORDER = [
    "pdf_parser",
    "table_extract",
    "page_fusion",
    "section_chunker",
    "vector_indexer",
    "clause_mapping",
    "schema_validation",
    "ingestion_eval",
    "llm_reviewer",
    "persist",
]


def extract_pages(src_pdf: str, dst_pdf: str, n_pages: int) -> int:
    """Extract first n_pages from src → dst. Return actual page count."""
    doc = fitz.open(src_pdf)
    total = len(doc)
    n = min(n_pages, total)
    out = fitz.open()
    out.insert_pdf(doc, from_page=0, to_page=n - 1)
    out.save(dst_pdf)
    out.close()
    doc.close()
    print(f"✓ Extracted pages 1–{n} from {Path(src_pdf).name} → {dst_pdf}")
    return n


def _load_cached_state(output_dir: Path, resume_from: str, pdf_path: str) -> dict:
    """Load cached node outputs up to (but not including) resume_from node.

    Returns a pre-populated IngestionState dict so the DAG can be
    re-run starting from ``resume_from``.
    """
    from backend.ingestion.dag import IngestionState

    # Determine which nodes to load from cache
    try:
        resume_idx = NODE_ORDER.index(resume_from)
    except ValueError:
        print(f"ERROR: unknown node '{resume_from}'. Valid: {NODE_ORDER}")
        sys.exit(1)

    nodes_to_load = NODE_ORDER[:resume_idx]

    state: IngestionState = {
        "pdf_path": str(Path(pdf_path).absolute()),
        "pages": [],
        "tables_per_page": [],
        "fused_pages": [],
        "section_chunks": [],
        "vector_index_info": {},
        "draft_rules": [],
        "validated_rules": [],
        "confidence": 0.0,
        "eval_metrics": {},
        "repair_count": 0,
    }

    for node in nodes_to_load:
        cache_file = output_dir / f"node_{node}.json"
        if not cache_file.exists():
            print(f"  ⚠  Cache miss: {cache_file.name} not found — "
                  f"cannot resume from '{resume_from}'")
            print(f"     Run a full pass first, then retry --resume {resume_from}")
            sys.exit(1)
        with open(cache_file) as f:
            node_output = json.load(f)
        # Merge node output into state
        for k, v in node_output.items():
            state[k] = v  # type: ignore[literal-required]
        print(f"  ↻  Loaded cached  {node:20s}  ({cache_file.name})")

    return state


def run_dag(pdf_path: str, output_dir: Path, *, resume_from: str | None = None) -> dict:
    """Run the full DAG and capture per-node state snapshots.

    If ``resume_from`` is set, load cached outputs for nodes before that
    point and run remaining nodes manually (bypassing LangGraph stream
    which always starts from the entry point).
    """
    from backend.ingestion.dag import (
        IngestionState, _build_graph,
        _node_pdf_parser, _node_table_extract, _node_page_fusion,
        _node_section_chunker, _node_vector_indexer, _node_clause_mapping,
        _node_schema_validation, _node_ingestion_eval, _node_llm_reviewer,
        _node_persist, _should_repair,
        REPAIR_MAX_RETRIES,
    )

    # Map node names → implementations for manual execution
    NODE_FUNCS = {
        "pdf_parser": _node_pdf_parser,
        "table_extract": _node_table_extract,
        "page_fusion": _node_page_fusion,
        "section_chunker": _node_section_chunker,
        "vector_indexer": _node_vector_indexer,
        "clause_mapping": _node_clause_mapping,
        "schema_validation": _node_schema_validation,
        "ingestion_eval": _node_ingestion_eval,
        "llm_reviewer": _node_llm_reviewer,
        "persist": _node_persist,
    }

    path = Path(pdf_path).absolute()

    # ── Build initial state (optionally from cache) ──
    if resume_from:
        initial = _load_cached_state(output_dir, resume_from, str(path))
        mode = f"RESUME from '{resume_from}'"
    else:
        initial: IngestionState = {
            "pdf_path": str(path),
            "pages": [],
            "tables_per_page": [],
            "fused_pages": [],
            "section_chunks": [],
            "vector_index_info": {},
            "draft_rules": [],
            "validated_rules": [],
            "confidence": 0.0,
            "eval_metrics": {},
            "repair_count": 0,
        }
        mode = "FULL (fresh)"

    print("\n" + "=" * 70)
    print("  LANGGRAPH DAG — END-TO-END INGESTION RUN")
    print("=" * 70)
    print(f"  PDF:   {pdf_path}")
    print(f"  Out:   {output_dir}")
    print(f"  Mode:  {mode}")
    print("=" * 70 + "\n")

    t0 = time.time()
    snapshots = {}

    if resume_from:
        # ── Manual node execution (bypasses LangGraph entry point) ──
        try:
            resume_idx = NODE_ORDER.index(resume_from)
        except ValueError:
            return {"status": "failed", "error": f"unknown node: {resume_from}"}

        state = dict(initial)
        nodes_to_run = NODE_ORDER[resume_idx:]

        for node_name in nodes_to_run:
            # Skip persist-only if it's the repair loop destination
            if node_name == "persist" and node_name not in nodes_to_run:
                continue

            elapsed = time.time() - t0
            func = NODE_FUNCS[node_name]
            try:
                node_output = func(state)
            except Exception as e:
                print(f"\n  [{elapsed:6.1f}s] ✗ {node_name} FAILED: {e}")
                import traceback
                traceback.print_exc()
                return {"status": "failed", "error": str(e), "elapsed_s": round(elapsed, 1)}

            # Merge output into state
            for k, v in node_output.items():
                state[k] = v

            elapsed = time.time() - t0
            summary = _summarise_node(node_name, node_output)
            snapshots[node_name] = {"elapsed_s": round(elapsed, 1), "summary": summary}
            print(f"  [{elapsed:6.1f}s] ✓ {node_name:20s}  {summary}")
            _save_node_output(output_dir, node_name, node_output)

            # ── Handle repair loop after llm_reviewer ──
            if node_name == "llm_reviewer":
                while _should_repair(state) == "repair":
                    rc = state.get("repair_count", 0)
                    print(f"  {'':8s}↻ Repair loop #{rc} (confidence={state.get('confidence', 0):.2f})")
                    # Re-run clause_mapping → schema → eval → reviewer
                    for repair_node in ["clause_mapping", "schema_validation", "ingestion_eval", "llm_reviewer"]:
                        repair_func = NODE_FUNCS[repair_node]
                        repair_out = repair_func(state)
                        for k, v in repair_out.items():
                            state[k] = v
                        elapsed = time.time() - t0
                        summary = _summarise_node(repair_node, repair_out)
                        print(f"  [{elapsed:6.1f}s]   ↻ {repair_node:18s}  {summary}")
                        _save_node_output(output_dir, repair_node, repair_out)

        final_state = state
    else:
        # ── Full run via LangGraph stream ──
        graph = _build_graph()
        final_state = dict(initial)

        try:
            for event in graph.stream(initial, stream_mode="updates"):
                for node_name, node_output in event.items():
                    elapsed = time.time() - t0
                    summary = _summarise_node(node_name, node_output)
                    snapshots[node_name] = {
                        "elapsed_s": round(elapsed, 1),
                        "summary": summary,
                    }
                    print(f"  [{elapsed:6.1f}s] ✓ {node_name:20s}  {summary}")
                    _save_node_output(output_dir, node_name, node_output)
                    for k, v in node_output.items():
                        final_state[k] = v
        except Exception as e:
            elapsed = time.time() - t0
            print(f"\n  [{elapsed:6.1f}s] ✗ DAG FAILED: {e}")
            import traceback
            traceback.print_exc()
            return {"status": "failed", "error": str(e), "elapsed_s": round(elapsed, 1)}

    elapsed = time.time() - t0

    # Build result
    validated = final_state.get("validated_rules") or []
    draft = final_state.get("draft_rules") or []
    confidence = final_state.get("confidence", 0)
    eval_metrics = final_state.get("eval_metrics") or {}
    status = final_state.get("status", "unknown")

    result = {
        "status": status,
        "rules_count": len(validated),
        "draft_rules_count": len(draft),
        "confidence": confidence,
        "eval_metrics": eval_metrics,
        "elapsed_s": round(elapsed, 1),
        "snapshots": snapshots,
    }
    if resume_from:
        result["resumed_from"] = resume_from

    # ── Print summary ──
    print(f"\n{'=' * 70}")
    print(f"  RESULT: status={status}  rules={len(validated)}  "
          f"confidence={confidence:.2f}  elapsed={elapsed:.1f}s")
    print(f"  Eval:   {json.dumps(eval_metrics, indent=2)}")
    print(f"{'=' * 70}")

    # ── Print per-section detail (new 12-field schema) ──
    if validated:
        print(f"\n  {'─' * 66}")
        print(f"  EXTRACTED TARIFF SECTIONS ({len(validated)}):")
        print(f"  {'─' * 66}")
        for i, s in enumerate(validated, 1):
            sid = s.get("id") or s.get("charge_name") or "?"
            sname = s.get("name") or s.get("charge_name") or ""
            calc = s.get("calculation") or {}
            ctype = calc.get("type") or s.get("basis") or "?" if isinstance(calc, dict) else "?"
            cbasis = calc.get("basis") or "" if isinstance(calc, dict) else ""
            cite = s.get("citation") or {}
            cpage = cite.get("page") or "?" if isinstance(cite, dict) else "?"
            csec = cite.get("section") or "" if isinstance(cite, dict) else ""
            minfee = s.get("minimum_fee")
            maxfee = s.get("maximum_fee")
            n_surcharges = len(s.get("surcharges") or [])
            n_exemptions = len(s.get("exemptions") or [])
            print(f"    {i}. {str(sid):30s}  type={str(ctype):25s}  basis={cbasis}")
            print(f"       name: {sname}")
            print(f"       page={cpage}  section={csec}  "
                  f"min_fee={minfee}  max_fee={maxfee}  "
                  f"surcharges={n_surcharges}  exemptions={n_exemptions}")
        print(f"  {'─' * 66}\n")

    # ── Print eval detail (matched / missed) ──
    matched = eval_metrics.get("matched") or []
    missed = eval_metrics.get("missed") or []
    if matched or missed:
        print(f"  {'─' * 66}")
        print(f"  EVAL DETAIL:")
        if matched:
            print(f"    Matched ({len(matched)}): {', '.join(matched)}")
        if missed:
            print(f"    Missed  ({len(missed)}): {', '.join(missed[:15])}"
                  + (f" ... +{len(missed)-15} more" if len(missed) > 15 else ""))
        print(f"  {'─' * 66}\n")

    # Save summary
    with open(output_dir / "result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)

    # Save validated rules
    if validated:
        with open(output_dir / "validated_rules.json", "w") as f:
            json.dump(validated, f, indent=2, default=str)
        print(f"  Validated rules → {output_dir / 'validated_rules.json'}")

    # Save draft rules
    if draft:
        with open(output_dir / "draft_rules.json", "w") as f:
            json.dump(draft, f, indent=2, default=str)
        print(f"  Draft rules     → {output_dir / 'draft_rules.json'}")

    return result


def _summarise_node(name: str, output: dict) -> str:
    """One-line summary of what a node produced."""
    if name == "pdf_parser":
        pages = output.get("pages") or []
        return f"{len(pages)} pages parsed"
    elif name == "table_extract":
        tables = output.get("tables_per_page") or []
        n_tables = sum(len(t.get("tables", [])) for t in tables)
        return f"{len(tables)} page extracts, {n_tables} tables"
    elif name == "page_fusion":
        fused = output.get("fused_pages") or []
        n_elems = sum(len(f.get("elements", [])) for f in fused)
        return f"{len(fused)} fused pages, {n_elems} elements"
    elif name == "section_chunker":
        chunks = output.get("section_chunks") or []
        tariff = [c for c in chunks if c.get("section_id", "0") != "0"]
        total_chars = sum(len(c.get("text", "")) for c in tariff)
        return f"{len(chunks)} chunks ({len(tariff)} tariff sections, {total_chars:,} chars)"
    elif name == "vector_indexer":
        info = output.get("vector_index_info") or {}
        n = info.get("chunk_count", 0)
        err = info.get("error", "")
        if err:
            return f"{n} chunks (FAISS skipped: {err[:60]})"
        return f"{n} chunks indexed in FAISS"
    elif name == "clause_mapping":
        rules = output.get("draft_rules") or []
        ids = [r.get("id", r.get("charge_name", "?")) for r in rules]
        return f"{len(rules)} draft sections: [{', '.join(ids)}]"
    elif name == "schema_validation":
        v = output.get("validated_rules") or []
        ids = [r.get("id", r.get("charge_name", "?")) for r in v]
        return f"{len(v)} validated: [{', '.join(ids)}]"
    elif name == "ingestion_eval":
        m = output.get("eval_metrics") or {}
        p = m.get("precision", 0)
        r = m.get("recall", 0)
        f1 = m.get("f1", 0)
        n_matched = len(m.get("matched", []))
        return f"P={p:.2f} R={r:.2f} F1={f1:.2f}  matched={n_matched}"
    elif name == "llm_reviewer":
        c = output.get("confidence", 0)
        rules = output.get("validated_rules") or []
        rc = output.get("repair_count", 0)
        return f"confidence={c:.2f}  rules={len(rules)}  repair_count={rc}"
    elif name == "persist":
        s = output.get("status", "?")
        m = output.get("message", "")
        return f"status={s}  {m[:50]}"
    return str(list(output.keys()))


def _save_node_output(output_dir: Path, node_name: str, output: dict):
    """Dump per-node state to JSON."""
    try:
        with open(output_dir / f"node_{node_name}.json", "w") as f:
            json.dump(output, f, indent=2, default=str)
    except Exception as e:
        print(f"    (warning: could not save {node_name} output: {e})")


def main():
    parser = argparse.ArgumentParser(
        description="E2E DAG test on Port Tariff PDF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Full fresh run
  python scripts/run_dag_e2e.py --pages 6

  # Resume from section_chunker (reuse cached pdf/table/fusion)
  python scripts/run_dag_e2e.py --pages 6 --resume section_chunker

  # Resume from clause_mapping (skip ~200s of Gemini extraction)
  python scripts/run_dag_e2e.py --pages 6 --resume clause_mapping
""",
    )
    parser.add_argument("--pages", type=int, default=6,
                        help="Number of pages to process (default: 6)")
    parser.add_argument("--pdf", type=str, default="storage/pdfs/Port Tariff.pdf",
                        help="Path to source PDF")
    parser.add_argument("--resume", type=str, default=None, metavar="NODE",
                        help=f"Resume from NODE (load earlier nodes from cache). "
                             f"Valid nodes: {', '.join(NODE_ORDER)}")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Quieten noisy libs
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("faiss").setLevel(logging.WARNING)

    src_pdf = args.pdf
    if not Path(src_pdf).exists():
        print(f"ERROR: PDF not found: {src_pdf}")
        sys.exit(1)

    # Create output dir
    output_dir = Path("output/dag_e2e")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract first N pages (needed even for --resume, for pdf_path)
    tmp_pdf = str(output_dir / f"port_tariff_first_{args.pages}_pages.pdf")
    if args.resume and Path(tmp_pdf).exists():
        print(f"✓ Reusing existing {tmp_pdf}")
    else:
        extract_pages(src_pdf, tmp_pdf, args.pages)

    # Run the DAG
    result = run_dag(tmp_pdf, output_dir, resume_from=args.resume)

    # Exit code based on result
    if result.get("status") in ("success", "low_confidence"):
        print("\n✓ DAG completed successfully")
        sys.exit(0)
    else:
        print(f"\n✗ DAG ended with status: {result.get('status')}")
        # Still exit 0 if we got validated rules (persist may fail due to embedding)
        rules_count = result.get("rules_count", 0)
        if rules_count > 0:
            print(f"  (got {rules_count} validated rules despite non-success status)")
            sys.exit(0)
        sys.exit(1)


if __name__ == "__main__":
    main()
