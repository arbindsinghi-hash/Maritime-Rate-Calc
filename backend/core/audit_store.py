"""
Audit Store — JSONL file-based audit trail.
============================================
Append-only log of every tariff calculation.
Each line is a JSON object with: id, vessel_name, imo_number, timestamp,
input_data, output_data, tariff_version.

File location: storage/audit/audit_log.jsonl
"""
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.core.config import settings


class AuditStore:
    """
    Thread-safe, file-based audit store using JSONL (one JSON record per line).
    No database required for POC.
    """

    def __init__(self, audit_dir: Optional[str] = None):
        self._dir = Path(audit_dir or os.path.join(settings.STORAGE_DIR, "audit"))
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / "audit_log.jsonl"
        self._lock = threading.Lock()

        # Initialize the file and count existing lines to set the next ID
        if not self._file.exists():
            self._file.touch()
        self._next_id = self._count_lines() + 1

    def _count_lines(self) -> int:
        """Count non-empty lines in the JSONL file."""
        if not self._file.exists():
            return 0
        with open(self._file, "r") as f:
            return sum(1 for line in f if line.strip())

    def append(
        self,
        vessel_name: str,
        imo_number: Optional[str],
        input_data: dict,
        output_data: Any,
        tariff_version: str,
    ) -> int:
        """
        Append an audit record. Returns the assigned integer ID (1-based).
        """
        with self._lock:
            audit_id = self._next_id
            record = {
                "id": audit_id,
                "vessel_name": vessel_name,
                "imo_number": imo_number,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "input_data": input_data,
                "output_data": output_data,
                "tariff_version": tariff_version,
            }
            with open(self._file, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
            self._next_id += 1
            return audit_id

    def get(self, audit_id: int) -> Optional[dict]:
        """
        Retrieve a single audit record by ID.
        Scans the file — fine for the expected scale (hundreds/thousands of records).
        """
        if not self._file.exists():
            return None
        with open(self._file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("id") == audit_id:
                    return record
        return None

    def list_recent(self, limit: int = 50) -> list[dict]:
        """Return the most recent `limit` audit records (newest first)."""
        if not self._file.exists():
            return []
        records = []
        with open(self._file, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return list(reversed(records[-limit:]))

    def clear(self):
        """Clear all audit records. Useful for tests."""
        with self._lock:
            with open(self._file, "w") as f:
                f.truncate(0)
            self._next_id = 1


# Module-level singleton
audit_store = AuditStore()
