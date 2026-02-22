"""
Chat Interaction Logger
=======================
Logs every chat interaction (user query, system prompt sent to LLM,
raw LLM response, parsed result or error) to:
  1. A JSONL file at {AUDIT_LOG_DIR}/chat_interactions.jsonl
  2. An in-memory ring buffer (most recent N entries) for the /prompts API

Controlled by ENABLE_PROMPT_PANEL in .env — when False the ring buffer
is not populated and the /prompts endpoint returns 404.
"""

import json
import logging
import os
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Maximum entries kept in memory for the developer panel
_MAX_RING_SIZE = 200


class ChatInteraction:
    """Single chat interaction record."""

    __slots__ = (
        "id", "timestamp", "user_message", "system_prompt",
        "raw_llm_response", "parsed_data", "error", "duration_ms",
    )

    def __init__(
        self,
        *,
        interaction_id: str,
        user_message: str,
        system_prompt: str,
        raw_llm_response: str | None = None,
        parsed_data: dict[str, Any] | None = None,
        error: str | None = None,
        duration_ms: float = 0.0,
    ):
        self.id = interaction_id
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.user_message = user_message
        self.system_prompt = system_prompt
        self.raw_llm_response = raw_llm_response
        self.parsed_data = parsed_data
        self.error = error
        self.duration_ms = duration_ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "user_message": self.user_message,
            "system_prompt": self.system_prompt,
            "raw_llm_response": self.raw_llm_response,
            "parsed_data": self.parsed_data,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


class ChatInteractionStore:
    """Thread-safe ring-buffer + JSONL file logger for chat interactions."""

    def __init__(self, log_dir: str, panel_enabled: bool = False):
        self._ring: deque[dict[str, Any]] = deque(maxlen=_MAX_RING_SIZE)
        self._lock = threading.Lock()
        self._panel_enabled = panel_enabled
        self._log_path: Path | None = None

        # Always create the JSONL log file (logging is always on)
        try:
            log_dir_path = Path(log_dir)
            log_dir_path.mkdir(parents=True, exist_ok=True)
            self._log_path = log_dir_path / "chat_interactions.jsonl"
            logger.info("Chat interaction log: %s", self._log_path)
        except Exception as e:
            logger.warning("Could not create chat interaction log dir: %s", e)

    @property
    def panel_enabled(self) -> bool:
        return self._panel_enabled

    def record(self, interaction: ChatInteraction) -> None:
        """Persist an interaction to JSONL and (if panel enabled) ring buffer."""
        entry = interaction.to_dict()

        # Always write to JSONL file
        if self._log_path:
            try:
                with open(self._log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, default=str) + "\n")
            except Exception as e:
                logger.warning("Failed to write chat interaction log: %s", e)

        # Populate ring buffer only when panel is enabled
        if self._panel_enabled:
            with self._lock:
                self._ring.append(entry)

    def get_recent(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return the most recent interactions (newest first)."""
        with self._lock:
            items = list(self._ring)
        # Return newest first
        items.reverse()
        return items[:limit]


# ── Module-level singleton (initialised lazily from settings) ────────────────
_store: ChatInteractionStore | None = None


def get_chat_log_store() -> ChatInteractionStore:
    """Get or create the singleton ChatInteractionStore."""
    global _store
    if _store is None:
        from backend.core.config import settings
        _store = ChatInteractionStore(
            log_dir=settings.AUDIT_LOG_DIR,
            panel_enabled=settings.ENABLE_PROMPT_PANEL,
        )
    return _store
