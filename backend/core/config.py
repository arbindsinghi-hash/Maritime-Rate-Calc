import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    # --- Audit Store ---
    AUDIT_LOG_DIR: str = os.getenv("AUDIT_LOG_DIR", "./storage/audit")

    # --- Gemini 2.5 Pro (primary VL extractor) ---
    GEMINI_API_BASE: str = os.getenv(
        "GEMINI_API_BASE",
        "https://generativelanguage.googleapis.com/v1beta/openai/",
    )
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

    # --- Gemini 2.5 Flash (chat extraction) ---
    GEMINI_CHAT_MODEL: str = os.getenv("GEMINI_CHAT_MODEL", "gemini-2.5-flash")

    # --- LLM Reasoning (gpt-oss-120b) ---
    LLM_API_BASE: str = os.getenv("LLM_API_BASE", "")
    LLM_API_KEY: str = os.getenv("LLM_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "openai/gpt-oss-120b")

    # --- Embedding ---
    EMBEDDING_API_BASE: str = os.getenv("EMBEDDING_API_BASE", "")
    EMBEDDING_API_KEY: str = os.getenv("EMBEDDING_API_KEY", "")
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "nvidia/llama-3.2-nv-embedqa-1b-v2")

    # --- LLM Behaviour ---
    LLM_TIMEOUT: int = int(os.getenv("LLM_TIMEOUT", "300"))
    GEMINI_TIMEOUT: int = int(os.getenv("GEMINI_TIMEOUT", "120"))   # per-page hard cap
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    LLM_TOP_P: float = float(os.getenv("LLM_TOP_P", "0.9"))

    # Storage Paths
    STORAGE_DIR: str = os.getenv("STORAGE_DIR", "./storage")
    PDF_DIR: str = os.getenv("PDF_DIR", "./storage/pdfs")
    YAML_DIR: str = os.getenv("YAML_DIR", "./storage/yaml")
    FAISS_INDEX_DIR: str = os.getenv("FAISS_INDEX_DIR", "./storage/faiss")

    # ---- Developer Prompt Panel ----
    ENABLE_PROMPT_PANEL: bool = os.getenv("ENABLE_PROMPT_PANEL", "false").lower() in ("true", "1", "yes")

    @property
    def audit_log_path(self) -> str:
        return os.path.join(self.AUDIT_LOG_DIR, "audit_log.jsonl")

    def validate_api_keys(self) -> list[str]:
        """Validate required API keys are set. Returns list of warnings."""
        warnings: list[str] = []
        if not self.GEMINI_API_KEY:
            warnings.append("GEMINI_API_KEY is not set — ingestion and chat extraction will fail")
        if not self.LLM_API_BASE:
            warnings.append("LLM_API_BASE is not set — LLM reviewer and chat enrichment will fail")
        if not self.LLM_API_KEY:
            warnings.append("LLM_API_KEY is not set — LLM reviewer and chat enrichment will fail")
        if not self.EMBEDDING_API_BASE:
            warnings.append("EMBEDDING_API_BASE is not set — semantic search will be unavailable")
        return warnings


settings = Settings()
