"""
Centralized LLM client factory.

Each model gets its own OpenAI-compatible client, cached as a singleton
to enable connection reuse across requests.

All config is pulled from env vars via Settings.
"""

from openai import AsyncOpenAI, OpenAI
from typing import Optional

from backend.core.config import settings

# Singleton cache keyed by (api_base, api_key, timeout)
_sync_cache: dict[tuple[str, str, int], OpenAI] = {}
_async_cache: dict[tuple[str, str, int], AsyncOpenAI] = {}


def _make_client(api_base: str, api_key: str, timeout: Optional[int] = None) -> OpenAI:
    effective_timeout = timeout or settings.LLM_TIMEOUT
    cache_key = (api_base, api_key, effective_timeout)
    if cache_key not in _sync_cache:
        _sync_cache[cache_key] = OpenAI(
            base_url=api_base, api_key=api_key, timeout=effective_timeout,
        )
    return _sync_cache[cache_key]


def _make_async_client(api_base: str, api_key: str, timeout: Optional[int] = None) -> AsyncOpenAI:
    effective_timeout = timeout or settings.LLM_TIMEOUT
    cache_key = (api_base, api_key, effective_timeout)
    if cache_key not in _async_cache:
        _async_cache[cache_key] = AsyncOpenAI(
            base_url=api_base, api_key=api_key, timeout=effective_timeout,
        )
    return _async_cache[cache_key]


# ── Gemini 2.5 Pro (primary VL extractor) ──────────────────────
def get_gemini_client() -> OpenAI:
    return _make_client(settings.GEMINI_API_BASE, settings.GEMINI_API_KEY,
                        timeout=settings.GEMINI_TIMEOUT)


def get_async_gemini_client() -> AsyncOpenAI:
    return _make_async_client(settings.GEMINI_API_BASE, settings.GEMINI_API_KEY,
                              timeout=settings.GEMINI_TIMEOUT)


# ── Gemini 2.5 Flash (chat extraction) ─────────────────────────
def get_gemini_chat_client(api_key: Optional[str] = None) -> OpenAI:
    """
    Return an OpenAI-compatible client pointed at Gemini for chat extraction.
    Uses the per-request api_key if provided, else falls back to env config.
    """
    key = api_key or settings.GEMINI_API_KEY
    return _make_client(settings.GEMINI_API_BASE, key,
                        timeout=settings.GEMINI_TIMEOUT)


# ── LLM Reasoning (kept for backward compat) ───────────────────
def get_llm_client() -> OpenAI:
    return _make_client(settings.LLM_API_BASE, settings.LLM_API_KEY)


def get_async_llm_client() -> AsyncOpenAI:
    return _make_async_client(settings.LLM_API_BASE, settings.LLM_API_KEY)


# ── Embedding ──────────────────────────────────────────────────
def get_embedding_client() -> OpenAI:
    return _make_client(settings.EMBEDDING_API_BASE, settings.EMBEDDING_API_KEY)
