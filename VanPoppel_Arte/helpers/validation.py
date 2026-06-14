"""
Validation gates for Arte invoice extraction.

The same checks judge BOTH the standard parser's output and any AI rescue
output, so a smarter re-extraction can never replace good data with bad:
  1. checksum     — item amounts + transport must equal the printed total
  2. completeness — every item needs an HS code, an amount and a net weight
  3. vocabulary   — the incoterm term must be one of the 11 real Incoterms
"""
import re

from global_db.functions.numbers.number_format import parse_number

INCOTERMS = {"EXW", "FCA", "FAS", "FOB", "CFR", "CIF", "CPT", "CIP", "DAP", "DPU", "DDP"}

_TOTAL_TOLERANCE = 0.05


def is_valid_incoterm(value) -> bool:
    if not value or not isinstance(value, str):
        return False
    return value.strip().split(" ", 1)[0].upper() in INCOTERMS


def _num(value, fmt) -> float:
    s = str(value if value is not None else "")
    s = re.sub(r"(KG|M2|EUR|USD)", "", s, flags=re.IGNORECASE).strip()
    return parse_number(s, fmt) or 0.0


def validate_invoice(items, footer, fmt):
    """Return a list of problems; empty list means the extraction passed all gates."""
    problems = []

    if not items:
        problems.append("no product lines extracted")
    for item in items or []:
        code = str(item.get("product_code") or "?")
        tariff_digits = re.sub(r"\D", "", str(item.get("customs_tariff") or ""))
        if len(tariff_digits) < 4:
            problems.append(f"item {code}: missing customs tariff (HS code)")
        if _num(item.get("amount"), fmt) <= 0:
            problems.append(f"item {code}: missing invoice amount")
        if not re.search(r"\d", str(item.get("unit_price") or "")):
            problems.append(f"item {code}: missing unit price")
        if _num(item.get("net_weight"), fmt) <= 0:
            problems.append(f"item {code}: missing net weight")

    total = footer.get("total") if footer else None
    if not total:
        problems.append("missing invoice total value")
    elif items:
        amount_sum = sum(_num(item.get("amount"), fmt) for item in items)
        transport = footer.get("transport") or 0.0
        if abs(amount_sum + transport - float(total)) > _TOTAL_TOLERANCE:
            problems.append(
                f"item amounts ({amount_sum:.2f}) + transport ({transport:.2f}) "
                f"do not add up to the invoice total ({float(total):.2f})")

    incoterm = (footer or {}).get("incoterm") or ""
    if not is_valid_incoterm(incoterm):
        problems.append(f"incoterm missing or invalid: '{incoterm}'" if incoterm
                        else "incoterm (delivery conditions) not found")

    return problems
