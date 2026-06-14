"""
Document-level number format detection and parsing (shared).

Invoices print numbers in either European format (dot = thousands, comma =
decimal: "2.089,34", "12,5") or US format (comma = thousands, dot = decimal:
"2,089.34"). A single value like "112.500" or "1.996" is ambiguous on its own,
so the format must be decided once per document from the unambiguous values it
contains, then applied to every value in that document.

Canonical version of VanPoppel_Soudal/helpers/number_format.py — keep in sync.
"""
import re

_NUMERIC = re.compile(r"\d[\d.,]*")
_EU_GROUPED = re.compile(r"\d{1,3}(\.\d{3})+(,\d+)?$")
_US_GROUPED = re.compile(r"\d{1,3}(,\d{3})+(\.\d+)?$")


def _clean(value) -> str:
    s = str(value).strip()
    s = re.sub(r"[€$£¥]", "", s)
    s = s.replace(" ", "").replace(" ", "").replace("'", "")
    while s and s[-1] in ".,":
        s = s[:-1]
    return s


def classify_number(value):
    """Classify one numeric string as 'EU', 'US' or None (ambiguous)."""
    s = _clean(value)
    if not s or not _NUMERIC.fullmatch(s):
        return None
    dots, commas = s.count("."), s.count(",")
    if not dots and not commas:
        return None
    if dots and commas:
        return "EU" if s.find(".") < s.find(",") else "US"
    if dots > 1:
        return "EU"  # repeated dots can only be thousands separators
    if commas > 1:
        return "US"
    # single separator: if it cannot be a thousands separator, it is a decimal
    sep = "." if dots else ","
    head, tail = s.split(sep)
    if len(tail) != 3 or not 1 <= len(head) <= 3:
        return "US" if sep == "." else "EU"
    return None  # e.g. "17.280" or "695,832" — ambiguous in isolation


def detect_format(strings, default="EU"):
    """Detect the document-wide number format from a collection of strings."""
    votes = {"EU": 0, "US": 0}
    for s in strings or []:
        verdict = classify_number(s)
        if verdict:
            votes[verdict] += 1
    if votes["EU"] != votes["US"]:
        return "EU" if votes["EU"] > votes["US"] else "US"
    return default


def parse_number(value, fmt="EU"):
    """Parse a numeric string using the document's format. Returns float or None."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = _clean(value)
    if not s or not _NUMERIC.fullmatch(s):
        return None
    if fmt == "EU":
        if "." in s:
            if _EU_GROUPED.fullmatch(s) or "," in s:
                s = s.replace(".", "").replace(",", ".")
            # else: lone dot that cannot be a thousands group — keep as decimal
        else:
            s = s.replace(",", ".")
    else:
        if "," in s:
            if _US_GROUPED.fullmatch(s) or "." in s:
                s = s.replace(",", "")
            else:
                s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None
