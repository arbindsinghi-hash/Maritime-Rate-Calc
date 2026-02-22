"""
LLM Reviewer Node.

Send draft rules to Gemini for review. Return confidence score and repaired rules.
If input has typos (e.g. in formula), output has them corrected.
"""

import json
import logging
import re
import time
from typing import List, Tuple

from backend.core.config import settings
from backend.core.llm_clients import get_gemini_client

logger = logging.getLogger(__name__)

RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5


def review_draft_rules(draft_rules: List[dict]) -> Tuple[float, List[dict]]:
    """
    Send draft tariff sections to Gemini for review and repair.

    Args:
        draft_rules: List of section dicts (id, name, calculation, etc.) or
                     legacy flat dicts (charge_name, rate, basis, formula, citation).

    Returns:
        (confidence: float in [0, 1], repaired_rules: list of dicts).
    """
    if not draft_rules:
        return 0.0, []

    client = get_gemini_client()
    model = settings.GEMINI_MODEL

    prompt = """Review and repair the following draft tariff sections extracted from a PDF.
For each section:
- Verify all numeric rates and fees are correct and consistent with the description.
- Fix typos in field names, rate values, or conditions.
- Ensure the calculation.type matches the actual rate structure.
- Ensure citation page/section is present.
- Check that exemptions and surcharges have valid condition strings.
Output a JSON object with two keys:
- "confidence": a number between 0 and 1 (1 = all sections look correct)
- "sections": the repaired list of sections (same structure as input)

Draft sections:
"""
    prompt += json.dumps(draft_rules, indent=2)

    content = ""
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 2):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4096,
                temperature=0.1,
            )
            content = (response.choices[0].message.content or "").strip()
            break
        except Exception as exc:
            last_exc = exc
            if attempt <= RETRY_ATTEMPTS:
                logger.warning(
                    "LLM reviewer call failed (attempt %d/%d): %s — retrying in %ds",
                    attempt, RETRY_ATTEMPTS + 1, exc, RETRY_DELAY_SECONDS,
                )
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                logger.error("LLM reviewer call failed after %d attempts: %s", attempt, exc)
                return 0.0, draft_rules

    confidence = 0.5
    repaired = draft_rules

    try:
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        data = json.loads(content)
        confidence = float(data.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))
        # Accept both "sections" (new) and "rules" (legacy) keys
        repaired_list = data.get("sections") or data.get("rules")
        if isinstance(repaired_list, list) and repaired_list:
            repaired = repaired_list
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("Failed to parse LLM reviewer JSON response: %s", exc)
        m = re.search(r"confidence[\"']?\s*:\s*([0-9.]+)", content, re.I)
        if m:
            confidence = max(0.0, min(1.0, float(m.group(1))))

    return confidence, repaired
