"""
Citation Service.

Resolves charge names to source PDF citations (page, section, bounding_box).
Citations are read from persisted tariff rules (YAML).
"""

from pathlib import Path
from typing import Optional

from backend.core.config import settings
from backend.models.schemas import Citation
from backend.models.tariff_rule import TariffRuleset


class CitationService:
    """
    Lookup citations by charge name (e.g. "Light Dues").
    Uses YAML tariff rules as source of truth.
    """

    def __init__(self, version: str = "latest"):
        self.version = version
        self._by_name: dict[str, Citation] = {}
        self._yaml_path = Path(settings.YAML_DIR) / f"tariff_rules_{version}.yaml"
        self._load_citations()

    def _load_citations(self) -> None:
        """Build charge_name -> Citation from YAML sections."""
        self._by_name.clear()
        if not self._yaml_path.exists():
            return
        ruleset = TariffRuleset.from_yaml(str(self._yaml_path))
        for section in ruleset.sections:
            if not section.citation:
                continue
            # Display name is the key the API and tests use (e.g. "Light Dues")
            name = section.name.strip()
            if not name:
                continue
            # tariff_rule.Citation has page, section; schemas.Citation adds bounding_box
            self._by_name[name] = Citation(
                page=section.citation.page,
                section=section.citation.section or "",
                bounding_box=None,
            )

    def get(self, charge_name: str) -> Optional[Citation]:
        """
        Return citation for a charge by its display name, or None if not found.
        """
        name = (charge_name or "").strip()
        return self._by_name.get(name)

    def get_page_bytes(self, pdf_filename: str, page: int) -> Optional[bytes]:
        """
        Optional: extract a single PDF page as raw bytes for frontend (e.g. pdf.js).
        pdf_filename: basename or path relative to PDF_DIR (e.g. "tariff.pdf").
        page: 1-based page number.
        Returns None if file missing or page out of range.
        """
        try:
            import pymupdf
        except ImportError:
            return None
        path = Path(settings.PDF_DIR)
        if not path.is_absolute():
            # Resolve relative to cwd; tests run from project root
            path = Path.cwd() / path
        candidate = path / pdf_filename
        if not candidate.exists():
            candidate = path / Path(pdf_filename).name
        if not candidate.exists():
            return None
        try:
            doc = pymupdf.open(str(candidate))
            try:
                if page < 1 or page > len(doc):
                    return None
                # PyMuPDF uses 0-based page index
                doc[page - 1]
                # Render page to PDF bytes (single-page document)
                single = pymupdf.open()
                single.insert_pdf(doc, from_page=page - 1, to_page=page - 1)
                buf = single.write()
                single.close()
                return buf
            finally:
                doc.close()
        except Exception:
            return None


# Singleton for use by API and tests
citation_service = CitationService()
