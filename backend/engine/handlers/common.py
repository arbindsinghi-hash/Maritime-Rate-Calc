"""
Shared helpers used across calculation handlers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from backend.models.schemas import Citation as ResponseCitation

if TYPE_CHECKING:
    from backend.models.tariff_rule import TariffSection


def build_citation(section: TariffSection) -> ResponseCitation:
    """Build a response Citation from a TariffSection."""
    if section.citation:
        return ResponseCitation(page=section.citation.page, section=section.citation.section)
    return ResponseCitation(page=0, section="")
