#!/usr/bin/env python3
"""
Resume the E2E DAG from cached node outputs — skips expensive Gemini calls.

Loads the state saved by a previous run_dag_e2e.py and re-runs only the
nodes that haven't completed successfully yet.

Usage:
    python scripts/resume_dag_e2e.py [--from-node ingestion_eval]
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

OUTPUT_DIR = Path("output/dag_e2e")

# Ordered list of all DAG nodes
ALL_NODES = [
    "pdf_parser",
    "table_extract",
    "page_fusion",
    "clause_mapping",
    "schema_validation",
    "ingestion_eval",
    "llm_reviewer",
    "persist",
]

# Map node name → the actual function from dag.py
NODE_FNS = {
    "pdf_parser": "backend.ingestion.dag:_node_pdf_parser",
    "table_extract": "backend.ingestion.dag:_node_table_extract",
    "page_fusion": "backend.ingestion.dag:_node_page_fusion",
    "clause_mapping": "backend.ingestion.dag:_node_clause_mapping",
    "schema_validation": "backend.ingestion.dag:_node_schema_validation",
    "ingestion_eval": "backend.ingestion.dag:_node_ingestion_eval",
    "llm_reviewer": "backend.ingestion.dag:_node_llm_reviewer",
    "persist": "backend.ingestion.dag:_node_persist",
}


def _load_node_fn(node_name: str):
    """Import and return the node function."""
    mod_path, fn_name = NODE_FNS[node_name].rsplit(":", 1)
    import importlib
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


def _load_cached_state(up_to_node: str) -> dict:
    """
    Build the cumulative state by loading cached node outputs
    in order up to (but not including) ``up_to_node``.
    """
    state: dict = {}
    for node in ALL_NODES:
        if node == up_to_node:
            break
        cache_file = OUTPUT_DIR / f"node_{node}.json"
        if cache_file.exists():
            with open(cache_file) as f:
                node_output = json.load(f)
            # Merge node output into state (each node returns {**state, ...new_keys})
            state.update(node_output)
            print(f"  ✓ Loaded cached {node:20s}  ({cache_file.stat().st_size / 1024:.0f} KB)")
        else:
            print(f"  ✗ No cache for {node} — stopping here")
            break
    return state


def _save_node_output(node_name: str, output: dict):
    """Save node output JSON."""
    try:
        with open(OUTPUT_DIR / f"node_{node_name}.json", "w") as f:
            json.dump(output, f, indent=2, default=str)
    except Exception as e:
        print(f"    (warning: could not save {node_name}: {e})")


def _summarise(name: str, output: dict) -> str:
    """One-line summary."""
    if name == "ingestion_eval":
        m = output.get("eval_metrics") or {}
        return f"precision={m.get('precision', 0):.2f}  recall={m.get('recall', 0):.2f}"
    elif name == "llm_reviewer":
        c = output.get("confidence", 0)
        rules = output.get("validated_rules") or []
        return f"confidence={c:.2f}  rules={len(rules)}"
    elif name == "persist":
        return f"status={output.get('status', '?')}"
    elif name == "clause_mapping":
        return f"{len(output.get('draft_rules', []))} draft rules"
    elif name == "schema_validation":
        return f"{len(output.get('validated_rules', []))} validated rules"
    return str(list(output.keys())[:5])


def main():
    parser = argparse.ArgumentParser(description="Resume DAG from cached state")
    parser.add_argument(
        "--from-node", type=str, default="ingestion_eval",
        choices=ALL_NODES,
        help="Node to resume from (default: ingestion_eval)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    from_node = args.from_node
    from_idx = ALL_NODES.index(from_node)
    remaining = ALL_NODES[from_idx:]

    print("=" * 70)
    print("  RESUME DAG — reusing cached Gemini outputs")
    print("=" * 70)
    print(f"  Resuming from:  {from_node}")
    print(f"  Nodes to run:   {' → '.join(remaining)}")
    print(f"  Cache dir:      {OUTPUT_DIR}")
    print("=" * 70)
    print()

    # Load cached state up to the resume point
    print("Loading cached state …")
    state = _load_cached_state(from_node)
    print()

    # Show what we have
    vr = state.get("validated_rules") or []
    dr = state.get("draft_rules") or []
    print(f"  Cached state: {len(state.get('pages', []))} pages, "
          f"{len(state.get('fused_pages', []))} fused, "
          f"{len(dr)} draft rules, {len(vr)} validated rules")
    print()

    # Run remaining nodes sequentially
    t0 = time.time()
    from backend.ingestion.dag import REPAIR_CONFIDENCE_THRESHOLD, REPAIR_MAX_RETRIES

    for node_name in remaining:
        # Handle repair loop: after llm_reviewer, check if we should loop
        if node_name == "clause_mapping" and state.get("repair_count", 0) > 0:
            # This is a repair iteration — check if we should continue
            confidence = state.get("confidence", 0.0)
            repair_count = state.get("repair_count", 0)
            if confidence >= REPAIR_CONFIDENCE_THRESHOLD:
                print(f"  → Confidence {confidence:.2f} ≥ {REPAIR_CONFIDENCE_THRESHOLD} — skipping to persist")
                # Jump to persist
                node_fn = _load_node_fn("persist")
                t1 = time.time()
                state = node_fn(state)
                elapsed = time.time() - t0
                summary = _summarise("persist", state)
                print(f"  [{elapsed:6.1f}s] ✓ {'persist':20s}  {summary}")
                _save_node_output("persist", state)
                break
            if repair_count >= REPAIR_MAX_RETRIES:
                print(f"  → Repair max retries ({REPAIR_MAX_RETRIES}) reached — persisting as low_confidence")
                node_fn = _load_node_fn("persist")
                t1 = time.time()
                state = node_fn(state)
                elapsed = time.time() - t0
                summary = _summarise("persist", state)
                print(f"  [{elapsed:6.1f}s] ✓ {'persist':20s}  {summary}")
                _save_node_output("persist", state)
                break

        node_fn = _load_node_fn(node_name)
        t1 = time.time()
        try:
            state = node_fn(state)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  [{elapsed:6.1f}s] ✗ {node_name:20s}  FAILED: {e}")
            import traceback
            traceback.print_exc()
            break
        elapsed = time.time() - t0
        summary = _summarise(node_name, state)
        print(f"  [{elapsed:6.1f}s] ✓ {node_name:20s}  {summary}")
        _save_node_output(node_name, state)

        # After llm_reviewer: decide repair vs persist
        if node_name == "llm_reviewer":
            confidence = state.get("confidence", 0.0)
            repair_count = state.get("repair_count", 0)
            if confidence < REPAIR_CONFIDENCE_THRESHOLD and repair_count < REPAIR_MAX_RETRIES:
                print(f"  → Confidence {confidence:.2f} < {REPAIR_CONFIDENCE_THRESHOLD}, "
                      f"repair {repair_count}/{REPAIR_MAX_RETRIES} — looping back to clause_mapping")
                # Loop back: clause_mapping → schema_validation → eval → reviewer
                remaining_loop = ["clause_mapping", "schema_validation", "ingestion_eval", "llm_reviewer"]
                for loop_node in remaining_loop:
                    node_fn = _load_node_fn(loop_node)
                    try:
                        state = node_fn(state)
                    except Exception as e:
                        elapsed = time.time() - t0
                        print(f"  [{elapsed:6.1f}s] ✗ {loop_node:20s}  FAILED: {e}")
                        break
                    elapsed = time.time() - t0
                    summary = _summarise(loop_node, state)
                    print(f"  [{elapsed:6.1f}s] ✓ {loop_node:20s}  {summary}")
                    _save_node_output(loop_node, state)

                # After the repair loop, go to persist regardless
                confidence = state.get("confidence", 0.0)
                repair_count = state.get("repair_count", 0)
                if confidence >= REPAIR_CONFIDENCE_THRESHOLD:
                    print(f"  → Confidence {confidence:.2f} ≥ threshold — proceeding to persist")
                else:
                    print(f"  → Still low ({confidence:.2f}) after {repair_count} repairs — persisting anyway")

            # Continue to persist (next in the for-loop or explicitly)
            if node_name == "llm_reviewer" and "persist" not in remaining[remaining.index(node_name)+1:]:
                # persist is next anyway
                pass

    total = time.time() - t0

    # Final report
    validated = state.get("validated_rules") or []
    confidence = state.get("confidence", 0)
    eval_metrics = state.get("eval_metrics") or {}
    status = state.get("status", "unknown")

    result = {
        "status": status,
        "rules_count": len(validated),
        "confidence": confidence,
        "eval_metrics": eval_metrics,
        "elapsed_s": round(total, 1),
        "resumed_from": from_node,
    }

    print(f"\n{'=' * 70}")
    print(f"  RESULT: status={status}  rules={len(validated)}  "
          f"confidence={confidence:.2f}  elapsed={total:.1f}s")
    print(f"  Eval:   {json.dumps(eval_metrics, indent=2)}")
    print(f"{'=' * 70}\n")

    with open(OUTPUT_DIR / "result.json", "w") as f:
        json.dump(result, f, indent=2, default=str)

    if validated:
        with open(OUTPUT_DIR / "validated_rules.json", "w") as f:
            json.dump(validated, f, indent=2, default=str)
        print(f"  Validated rules → {OUTPUT_DIR / 'validated_rules.json'}")

    if status in ("success", "low_confidence"):
        print("\n✓ DAG resumed and completed successfully")
        sys.exit(0)
    else:
        print(f"\n✗ DAG ended with status: {status}")
        sys.exit(1)


if __name__ == "__main__":
    main()
