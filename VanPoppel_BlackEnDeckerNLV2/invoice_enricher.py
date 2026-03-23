"""
invoice_enricher.py  (v2)
-------------------------
Enriches Excel line-item rows with InvoiceNumber and InvoiceDate by
parsing the DI OCR output of a multi-invoice PDF.

Key insight
-----------
* Invoice headers (number + date) live in the *running text* between
  tables, not inside the item tables.
* Item tables immediately follow their header in the PDF, so we can
  split the text into invoice blocks and pair each block's items with
  its header.
* The total count of Excel rows == total count of PDF invoice lines
  (always 30 = 30 in this workflow), so we use greedy bipartite
  matching to guarantee every Excel row gets an assignment.

Pipeline
--------
1. parse_invoice_blocks(di_result)
   → list of InvoiceBlock  (header + flat list of PDFLine)

2. build_candidate_pool(blocks)
   → flat list of all PDFLine objects (each carries invoice_number/date)

3. enrich_items(di_result, item_rows)
   → for each Excel row, find the best-scoring unassigned PDFLine;
     if no perfect match exists, force-assign the best remaining one.
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
#  Tolerances
# ─────────────────────────────────────────────────────────
_WEIGHT_TOL  = 0.06   # 6 % relative tolerance on net weight
_AMOUNT_TOL  = 0.05   # 5 % relative tolerance on amount


# ─────────────────────────────────────────────────────────
#  Data classes
# ─────────────────────────────────────────────────────────

@dataclass
class PDFLine:
    """One invoice line item parsed from the PDF."""
    invoice_number: str
    invoice_date:   str
    commodity:      str          # cleaned HS code
    net_weight:     float | None
    amount:         float | None
    description:    str          # article / reference text
    origin:         str
    _used: bool = field(default=False, repr=False)


@dataclass
class InvoiceBlock:
    """One invoice: its header + its line items."""
    invoice_number: str
    invoice_date:   str
    lines: list[PDFLine] = field(default_factory=list)


# ─────────────────────────────────────────────────────────
#  Small helpers
# ─────────────────────────────────────────────────────────

def _clean_hs(value: Any) -> str:
    if value is None:
        return ""
    s = re.sub(r"[^0-9]", "", str(value))
    return s.strip()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    s = re.sub(r"[^\d.,\-]", "", str(value))
    # handle European-style thousands separator: 1.234,56 → 1234.56
    if "," in s and "." in s:
        if s.index(",") > s.index("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _norm_date(raw: str) -> str:
    """Normalise various date formats to dd/mm/yyyy."""
    raw = raw.strip()
    # dd/mm/yyyy or dd-mm-yyyy
    m = re.match(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})", raw)
    if m:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"
    # yyyy-mm-dd
    m = re.match(r"(\d{4})[/\-\.](\d{2})[/\-\.](\d{2})", raw)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return raw


def _within_tol(a: float | None, b: float | None, tol: float) -> bool:
    if a is None or b is None or a == 0 and b == 0:
        return a == b
    denom = max(abs(a), abs(b))
    return denom > 0 and abs(a - b) / denom <= tol


# ─────────────────────────────────────────────────────────
#  Step 1: split markdown text into invoice blocks
# ─────────────────────────────────────────────────────────

# Matches: "Invoice No: 9600003567" / "Invoice Number 9600003567" etc.
_INVOICE_HDR_RE = re.compile(
    r"(?:invoice\s*(?:no\.?|number|#|nr\.?)?[\s:\-]+)(\d{5,20})",
    re.IGNORECASE,
)

# Matches dates in text
_DATE_RE = re.compile(
    r"\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4}|\d{4}[/\-\.]\d{2}[/\-\.]\d{2})\b"
)

# HS code: 8+ digit string, optionally dotted
_HS_RE = re.compile(r"\b(\d{4}[.\s]?\d{2}[.\s]?\d{2,4})\b")


def _parse_blocks_from_text(text: str, known_invoices: list[str]) -> list[InvoiceBlock]:
    """
    Split the markdown_content into invoice blocks by detecting
    "Invoice No/Number/Nr" headers in the running text.
    If `known_invoices` is provided, we directly search for those exact numbers.

    Each block collects text lines until the next invoice header.
    The item lines within a block are extracted by finding HS codes.
    """
    blocks: list[InvoiceBlock] = []
    current_block: InvoiceBlock | None = None
    current_date = ""

    known_re = None
    if known_invoices:
        # Create a regex that matches any of the known exact invoice numbers as whole words
        pattern = r"\b(" + "|".join(re.escape(inv) for inv in known_invoices) + r")\b"
        known_re = re.compile(pattern, re.IGNORECASE)

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # ── Invoice header detection ──────────────────────────────────
        inv_nr = None
        if known_re:
            m = known_re.search(line)
            if m:
                inv_nr = m.group(1).strip()
        
        if not inv_nr:
            inv_match = _INVOICE_HDR_RE.search(line)
            if inv_match:
                inv_nr = inv_match.group(1).strip()

        if inv_nr:
            # Try to grab the date from the same line or remember if we
            # saw one recently
            date_m = _DATE_RE.search(line)
            if date_m:
                current_date = _norm_date(date_m.group(1))

            # Start a new block
            current_block = InvoiceBlock(
                invoice_number=inv_nr,
                invoice_date=current_date,
            )
            blocks.append(current_block)
            continue

        # ── Date update (standalone date line, or date near other text) ─
        date_m = _DATE_RE.search(line)
        if date_m:
            current_date = _norm_date(date_m.group(1))
            # Patch the current block's date if it has none yet
            if current_block and not current_block.invoice_date:
                current_block.invoice_date = current_date

        if current_block is None:
            continue

        # ── HS code line inside a block ─────────────────────────────
        hs_m = _HS_RE.search(line)
        if hs_m:
            hs = _clean_hs(hs_m.group(1))
            if len(hs) < 8:
                continue  # skip short matches (years, zip codes, …)

            # Extract floats from the rest of the line
            nums = [_to_float(n) for n in re.findall(r"\d[\d\s,\.]*(?:\.\d+)?", line)]
            nums = [n for n in nums if n is not None and n > 0]

            # Heuristic: net weight is usually < 10 000 and amount is the last number
            amount     = nums[-1] if nums else None
            net_weight = nums[-2] if len(nums) >= 2 else None

            current_block.lines.append(PDFLine(
                invoice_number=current_block.invoice_number,
                invoice_date=current_block.invoice_date,
                commodity=hs,
                net_weight=net_weight,
                amount=amount,
                description=line,
                origin="",
            ))

    logger.info(
        f"_parse_blocks_from_text: found {len(blocks)} invoice block(s) "
        f"with {sum(len(b.lines) for b in blocks)} total line(s)"
    )
    return blocks


# ─────────────────────────────────────────────────────────
#  Step 2: extract from DI tables (supplement text parsing)
# ─────────────────────────────────────────────────────────

_HS_HEADER_HINTS  = {"commodity", "hs", "hs code", "hscode", "tariff",
                     "customs code", "commodity code", "hs-code", "artikel"}
_NET_HEADER_HINTS = {"net", "net weight", "net wt", "netweight", "netwicht", "netto"}
_AMT_HEADER_HINTS = {"amount", "total", "value", "invoice value",
                     "bedrag", "totaal", "total amount"}
_DESC_HINTS       = {"description", "omschrijving", "article", "goods",
                     "product", "item description", "artikel"}
_ORIG_HINTS       = {"origin", "herkomst", "country of origin"}


def _parse_blocks_from_tables(
    tables: list[list[list[str]]],
    text_blocks: list[InvoiceBlock],
    known_invoices: list[str],
) -> list[InvoiceBlock]:
    """
    Walk DI tables and try to attach items to existing text-parsed
    invoice blocks (matched by invoice number appearing inside the table
    or by sequential order).

    Returns the same `text_blocks` list with table items merged in,
    or a new set of blocks if text parsing found nothing.
    """
    # Build a lookup: invoice_number → block
    block_by_nr: dict[str, InvoiceBlock] = {
        b.invoice_number: b for b in text_blocks
    }

    def _col_of(row: list[str], hints: set[str]) -> int | None:
        for i, cell in enumerate(row):
            if cell.lower().strip() in hints:
                return i
        return None

    known_re = None
    if known_invoices:
        pattern = r"\b(" + "|".join(re.escape(inv) for inv in known_invoices) + r")\b"
        known_re = re.compile(pattern, re.IGNORECASE)

    for grid in tables:
        if not grid:
            continue

        current_inv_nr   = ""
        current_inv_date = ""
        col_hs = col_net = col_amt = col_desc = col_orig = None
        header_found = False

        for row in grid:
            row_text = " ".join(row)
            row_lower = [c.lower().strip() for c in row]

            # Check if this row is the item header row
            if not header_found:
                _ch = _col_of(row_lower, _HS_HEADER_HINTS)
                if _ch is not None:
                    col_hs   = _ch
                    col_net  = _col_of(row_lower, _NET_HEADER_HINTS)
                    col_amt  = _col_of(row_lower, _AMT_HEADER_HINTS)
                    col_desc = _col_of(row_lower, _DESC_HINTS)
                    col_orig = _col_of(row_lower, _ORIG_HINTS)
                    header_found = True
                    continue

                # Look for invoice number / date in pre-header rows
                if known_re:
                    m = known_re.search(row_text)
                    if m:
                        current_inv_nr = m.group(1)
                if not current_inv_nr:
                    inv_m = _INVOICE_HDR_RE.search(row_text)
                    if inv_m:
                        current_inv_nr = inv_m.group(1)

                date_m = _DATE_RE.search(row_text)
                if date_m:
                    current_inv_date = _norm_date(date_m.group(1))
                continue

            # ── Data rows ───────────────────────────────────────────
            if col_hs is None or col_hs >= len(row):
                continue

            # Refresh invoice context from inline cells
            if known_re:
                m = known_re.search(row_text)
                if m:
                    current_inv_nr = m.group(1)
            
            if not current_inv_nr:
                inv_m = _INVOICE_HDR_RE.search(row_text)
                if inv_m:
                    current_inv_nr = inv_m.group(1)

            date_m = _DATE_RE.search(row_text)
            if date_m:
                current_inv_date = _norm_date(date_m.group(1))

            hs = _clean_hs(row[col_hs])
            if len(hs) < 8:
                continue

            net  = _to_float(row[col_net])  if col_net  is not None and col_net  < len(row) else None
            amt  = _to_float(row[col_amt])  if col_amt  is not None and col_amt  < len(row) else None
            desc = row[col_desc].strip()     if col_desc is not None and col_desc < len(row) else ""
            orig = row[col_orig].strip()     if col_orig is not None and col_orig < len(row) else ""

            pdf_line = PDFLine(
                invoice_number=current_inv_nr,
                invoice_date=current_inv_date,
                commodity=hs,
                net_weight=net,
                amount=amt,
                description=desc,
                origin=orig,
            )

            # Attach to the correct block
            if current_inv_nr in block_by_nr:
                block_by_nr[current_inv_nr].lines.append(pdf_line)
            elif text_blocks:
                # No header found — attach to the last block seen
                text_blocks[-1].lines.append(pdf_line)
            else:
                # Create a synthetic block
                blk = InvoiceBlock(
                    invoice_number=current_inv_nr,
                    invoice_date=current_inv_date,
                    lines=[pdf_line],
                )
                text_blocks.append(blk)
                block_by_nr[current_inv_nr] = blk

    return text_blocks


# ─────────────────────────────────────────────────────────
#  Step 3: score a PDF line against an Excel row
# ─────────────────────────────────────────────────────────

def _score(pdf: PDFLine,
           excel_hs:   str,
           excel_net:  float | None,
           excel_amt:  float | None,
           excel_desc: str,
           excel_orig: str) -> int:
    """
    Return a match score. Higher is better.
    Only lines with the same HS code are ever scored.
    A score ≥ 1 means at least one secondary field agrees.
    """
    score = 0

    # Net weight
    if _within_tol(pdf.net_weight, excel_net, _WEIGHT_TOL):
        score += 4
    elif excel_net is not None and pdf.net_weight is not None:
        score -= 3

    # Amount / invoice value
    if _within_tol(pdf.amount, excel_amt, _AMOUNT_TOL):
        score += 4
    elif excel_amt is not None and pdf.amount is not None:
        score -= 3

    # Article / description overlap
    if excel_desc and pdf.description:
        my_words  = set(excel_desc.upper().split())
        pdf_words = set(pdf.description.upper().split())
        overlap   = my_words & pdf_words
        score    += min(len(overlap), 3)   # cap at 3 to not overpower weight/amount

    # Origin
    if excel_orig and pdf.origin:
        if excel_orig.strip().upper() == pdf.origin.strip().upper():
            score += 2
        else:
            score -= 1

    return score


# ─────────────────────────────────────────────────────────
#  Public API
# ─────────────────────────────────────────────────────────

def enrich_items(
    di_result:  dict,
    item_rows:  list[dict],
    known_invoices: list[str] = None,
) -> list[dict]:
    """
    Enrich each dict in `item_rows` with InvoiceNumber and InvoiceDate.

    Parameters
    ----------
    di_result : dict
        Output of DILayoutClient.analyze_layout().
        Expected keys: tables (list), markdown_content (str).
    item_rows : list[dict]
        Each dict must have "HSCode". Optionally "NetWeight", "Amount",
        "Description", "Origin".
    known_invoices : list[str]
        Optional list of expected explicit invoice numbers (from Excel header).

    Returns
    -------
    list[dict]
        Same list with every dict updated in-place:
            InvoiceNumber  : str
            InvoiceDate    : str
            match_status   : "matched" | "ambiguous" | "force_matched"
    """
    tables           = di_result.get("tables", [])
    markdown_content = di_result.get("markdown_content", "")
    known_invoices   = known_invoices or []

    # ── Phase 1: parse invoice blocks from running text ───────────────
    text_blocks = _parse_blocks_from_text(markdown_content, known_invoices)

    # ── Phase 2: supplement/correct with table data ───────────────────
    all_blocks  = _parse_blocks_from_tables(tables, text_blocks, known_invoices)

    # Flatten to candidate pool
    pool: list[PDFLine] = []
    for blk in all_blocks:
        for ln in blk.lines:
            # Make sure invoice_number/date are inherited from block
            if not ln.invoice_number:
                ln.invoice_number = blk.invoice_number
            if not ln.invoice_date:
                ln.invoice_date   = blk.invoice_date
            pool.append(ln)

    logger.info(
        f"enrich_items: {len(all_blocks)} invoice blocks, "
        f"{len(pool)} PDF lines total, "
        f"{len(item_rows)} Excel rows to enrich"
    )

    if not pool:
        logger.warning("enrich_items: empty PDF candidate pool – cannot enrich")
        for row in item_rows:
            row.setdefault("InvoiceNumber", "")
            row.setdefault("InvoiceDate",   "")
            row["match_status"] = "no_pdf_data"
        return item_rows

    # ── Phase 3: greedy bipartite matching ───────────────────────────
    # Pass 1: for each Excel row, score all same-HS candidates and find
    # the best. Only assign if there is an unambiguous winner.
    # Pass 2: resolve remaining rows using the best available (even if
    # already used in a lower-priority assignment).

    # pending[i] = (excel_index, hs, net, amt, desc, orig)
    pending = []
    for i, row in enumerate(item_rows):
        hs   = _clean_hs(row.get("HSCode", ""))
        net  = _to_float(row.get("NetWeight"))
        amt  = _to_float(row.get("Amount"))
        desc = str(row.get("Description", ""))
        orig = str(row.get("Origin", ""))
        pending.append((i, hs, net, amt, desc, orig))

    assignments: dict[int, PDFLine | None] = {i: None for i, *_ in pending}

    # ── Pass 1: greedy high-confidence assignment ────────────────────
    # Sort Excel rows by how many PDF candidates share their HS code
    # (fewest candidates first → resolve the easiest matches first)
    def _candidate_count(item):
        _, hs, *_ = item
        return sum(1 for p in pool if _clean_hs(p.commodity) == hs and not p._used)

    sorted_pending = sorted(pending, key=_candidate_count)

    for i, hs, net, amt, desc, orig in sorted_pending:
        same_hs = [p for p in pool if _clean_hs(p.commodity) == hs and not p._used]
        if not same_hs:
            continue

        scored = sorted(
            [(p, _score(p, hs, net, amt, desc, orig)) for p in same_hs],
            key=lambda x: x[1],
            reverse=True,
        )
        best_score   = scored[0][1]
        best_matches = [p for p, s in scored if s == best_score]

        if len(best_matches) == 1:
            winner = best_matches[0]
            winner._used      = True
            assignments[i]    = winner
        # else: leave for pass 2

    # ── Pass 2: force-assign remaining rows ─────────────────────────
    # For rows still unassigned, pick the highest-scoring unused candidate
    # with the same HS code; if none unused, allow reuse (last resort).
    for i, hs, net, amt, desc, orig in pending:
        if assignments[i] is not None:
            continue

        same_hs_unused = [p for p in pool if _clean_hs(p.commodity) == hs and not p._used]
        same_hs_any    = [p for p in pool if _clean_hs(p.commodity) == hs]

        candidates = same_hs_unused or same_hs_any
        if not candidates:
            # No HS match at all — pick global best by amount/weight
            candidates = [p for p in pool if not p._used] or pool

        scored = sorted(
            [(p, _score(p, hs, net, amt, desc, orig)) for p in candidates],
            key=lambda x: x[1],
            reverse=True,
        )
        winner = scored[0][0]
        winner._used   = True
        assignments[i] = winner

    # ── Phase 4: write results back ──────────────────────────────────
    matched = ambiguous = force_matched = 0
    for i, row in enumerate(item_rows):
        winner = assignments.get(i)
        if winner is None:
            row["InvoiceNumber"] = ""
            row["InvoiceDate"]   = ""
            row["match_status"]  = "unresolved"
            continue

        row["InvoiceNumber"] = winner.invoice_number or ""
        row["InvoiceDate"]   = winner.invoice_date   or ""

        # Classify quality
        _, hs, net, amt, desc, orig = pending[i]
        sc = _score(winner, hs, net, amt, desc, orig)
        if sc >= 4:
            row["match_status"] = "matched"
            matched += 1
        elif sc >= 0:
            row["match_status"] = "force_matched"
            force_matched += 1
        else:
            row["match_status"] = "ambiguous"
            ambiguous += 1

    logger.info(
        f"enrich_items result: {matched} matched, "
        f"{force_matched} force_matched, {ambiguous} ambiguous "
        f"out of {len(item_rows)} rows"
    )
    return item_rows
