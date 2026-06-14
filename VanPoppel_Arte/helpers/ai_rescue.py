"""
AI re-extraction for Arte invoices whose standard parsing failed validation.

Layout-independent fallback: the full document text goes to GPT-4o with a
strict JSON schema. The caller MUST re-validate the result with the same
gates (validation.validate_invoice) before accepting it — AI output is never
trusted on its own.
"""
import json
import logging

from AI_agents.OpenAI.custom_call import CustomCall

_TEXT_FIELDS = ("product_code", "product_name", "order_number", "reference",
                "customs_tariff", "origin", "unit")

_ROLE = (
    "You are a precise customs-document extraction engine for ARTE invoices. "
    "You return only valid JSON. All numbers must be plain JSON numbers with a dot "
    "as decimal separator and no thousands separators."
)

_PROMPT_TEMPLATE = """The automated parser could not fully extract this invoice. Detected problems:
{problems}

Re-extract ALL product lines and the totals/incoterm from the invoice text below.

Return ONLY this JSON structure (no markdown, no explanations):
{{
  "items": [
    {{
      "product_code": "the 7-digit article code",
      "product_name": "...",
      "order_number": "...",
      "reference": "...",
      "customs_tariff": "customs tariff / HS code, digits only",
      "net_weight": <number, in KG>,
      "surface": <number, in M2, 0 if not stated>,
      "quantity": <number>,
      "unit": "unit of the quantity (e.g. M, M2, PC)",
      "unit_price": <number>,
      "amount": <number, the line total>,
      "origin": "country of origin exactly as written"
    }}
  ],
  "footer": {{
    "incoterm": "<3-letter Incoterm> <place>",
    "total": <number, the invoice total>,
    "currency": "EUR, USD, ...",
    "transport": <number, transport/freight cost, 0 if not stated>
  }}
}}

Rules:
- Extract EVERY product line. Do not skip, merge or invent any.
- The incoterm 3-letter code must be one of: EXW, FCA, FAS, FOB, CFR, CIF, CPT, CIP, DAP, DPU, DDP.
- Numbers: dot as decimal separator, no thousands separators, no units, no currency symbols.
- If a field is truly absent from the document, use null.

Invoice text:
\"\"\"
{text}
\"\"\"
"""


def rescue_invoice(full_text, problems):
    """Ask GPT-4o for a structured re-extraction. Returns {items, footer}."""
    prompt = _PROMPT_TEMPLATE.format(problems="\n".join(f"- {p}" for p in problems), text=full_text)
    response = CustomCall().send_request(_ROLE, prompt)
    cleaned = response.replace("```json", "").replace("```", "").strip()
    data = json.loads(cleaned)

    items = data.get("items") or []
    for item in items:
        # text fields must be strings — downstream cleaning calls .strip() on them
        for key in _TEXT_FIELDS:
            value = item.get(key)
            item[key] = "" if value is None else str(value)
    footer = data.get("footer") or {}
    if footer.get("incoterm") is not None:
        footer["incoterm"] = str(footer["incoterm"]).strip()

    logging.info(f"AI rescue extracted {len(items)} item(s), footer: { {k: footer.get(k) for k in ('incoterm', 'total', 'currency', 'transport')} }")
    return {"items": items, "footer": footer}
