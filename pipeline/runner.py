#!/usr/bin/env python3
"""
MRCA AI Tariff — Pipeline Runner

Loads pipeline/pipeline.yaml and runs phases/steps. Modular and containerized:
- Steps with container: backend → run via `docker compose run --rm backend <run>`
- Steps with container: host → run in local shell

Usage:
  python pipeline/runner.py              # run all phases in order
  python pipeline/runner.py --phase 1    # run only phase 1
  python pipeline/runner.py --list       # list phases and steps
  python pipeline/runner.py --dry-run    # print commands only
  python pipeline/runner.py --parallel   # run parallel_groups where possible (phase 2,3,4 after 1)

Requires: PyYAML. Run from mrca-ai-tariff project root.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# Project root = parent of pipeline/
PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent


def load_pipeline(path: Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def resolve_phase_order(phases: list, parallel_groups: list | None) -> list[list[str]]:
    """Return list of phase-id lists: each inner list can run in parallel."""
    order = []
    done = set()
    phase_ids = [p["id"] for p in phases]
    deps = {p["id"]: set(p.get("depends_on", [])) for p in phases}

    while len(done) < len(phase_ids):
        ready = [
            pid for pid in phase_ids
            if pid not in done and deps[pid].issubset(done)
        ]
        if not ready:
            break
        # If parallel_groups says these can run together, group them
        if parallel_groups:
            for group in parallel_groups:
                if set(group).issubset(set(ready)):
                    order.append(group)
                    done.update(group)
                    ready = [r for r in ready if r not in group]
                    break
            else:
                order.append(ready[:1])
                done.add(ready[0])
        else:
            order.append(ready)
            done.update(ready)
    return order


def run_cmd(cmd: str, cwd: Path, dry_run: bool, container: str | None, compose_file: str) -> int:
    if dry_run:
        print(f"  [dry-run] {cmd}")
        return 0
    # When runner is already inside backend container (e.g. docker compose run backend python pipeline/runner.py)
    inside_container = os.environ.get("RUNNER_INSIDE_CONTAINER", "").lower() in ("1", "true", "yes")
    if container == "backend" and not inside_container:
        full = f"docker compose -f {compose_file} run --rm --no-deps backend sh -c {repr(cmd)}"
        return subprocess.call(full, shell=True, cwd=cwd)
    if container == "host" and inside_container:
        print(f"  [skip host step] {cmd}", file=sys.stderr)
        return 0
    return subprocess.call(cmd, shell=True, cwd=cwd)


def main():
    ap = argparse.ArgumentParser(description="MRCA AI Tariff pipeline runner")
    ap.add_argument("--phase", type=int, default=None, help="Run only phase N (1-8)")
    ap.add_argument("--list", action="store_true", help="List phases and steps")
    ap.add_argument("--dry-run", action="store_true", help="Print commands only")
    ap.add_argument("--parallel", action="store_true", help="Run parallel_groups (2,3,4 in parallel)")
    ap.add_argument("--pipeline", type=Path, default=PIPELINE_DIR / "pipeline.yaml", help="Pipeline YAML path")
    args = ap.parse_args()

    pipeline_path = args.pipeline.resolve()
    if not pipeline_path.exists():
        print(f"Pipeline not found: {pipeline_path}", file=sys.stderr)
        sys.exit(1)

    data = load_pipeline(pipeline_path)
    phases = data["phases"]
    env = data.get("env", {})
    compose_file = env.get("COMPOSE_FILE", "docker-compose.yml")
    project_root = PROJECT_ROOT

    if args.list:
        for p in phases:
            print(f"{p['id']}: {p['name']} (depends: {p.get('depends_on', [])})")
            for s in p.get("steps", []):
                print(f"  - {s['id']}: {s['name']} (container: {s.get('container', 'host')})")
        return 0

    # Phase filter
    if args.phase is not None:
        idx = args.phase - 1
        if idx < 0 or idx >= len(phases):
            print(f"Phase must be 1..{len(phases)}", file=sys.stderr)
            sys.exit(1)
        phases_to_run = [phases[idx]]
        # Single phase: run only that phase (ignore deps)
        order = [[phases_to_run[0]["id"]]]
    else:
        phases_to_run = phases
        order = resolve_phase_order(phases_to_run, data.get("parallel_groups") if args.parallel else None)
    failed = []

    for batch in order:
        for phase_id in batch:
            phase = next((p for p in phases if p["id"] == phase_id), None)
            if not phase:
                continue
            print(f"\n--- {phase['id']}: {phase['name']} ---")
            for step in phase.get("steps", []):
                if not step.get("required", True):
                    continue
                cmd = step.get("run", "")
                container = step.get("container", "host")
                code = run_cmd(cmd, project_root, args.dry_run, container, compose_file)
                if code != 0:
                    failed.append(f"{phase_id}.{step['id']}")
                    if not args.dry_run:
                        print(f"FAILED: {phase_id}.{step['id']}", file=sys.stderr)
                        sys.exit(1)

    if failed:
        print("Failed steps:", failed, file=sys.stderr)
        sys.exit(1)
    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
