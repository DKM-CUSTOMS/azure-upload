"""
Deterministic extraction from Soudal PDFs using the embedded text layer.

These SAP-generated PDFs carry an exact text layer, so the numbers, the
incoterm line, the origin declaration and the commodity-code summary can be
read without OCR or AI. The extra file additionally contains subtotal (**)
and grand-total (***) rows that act as checksums: after parsing, the line
rows must sum to them, which both validates the extraction and disambiguates
the number format.
"""
import logging
import re

import fitz

try:
    from VanPoppel_Soudal.helpers.number_format import detect_format, parse_number
except ImportError:  # standalone execution (tests)
    from number_format import detect_format, parse_number

_TOLERANCE = 0.02
_WEIGHT_FIELDS = ("Gross", "Net weight", "Net Value")


def _row_lines(pdf_source):
    """Rebuild visual table rows from word coordinates (text mode scrambles columns)."""
    if isinstance(pdf_source, (bytes, bytearray)):
        doc = fitz.open(stream=bytes(pdf_source), filetype="pdf")
    else:
        doc = fitz.open(pdf_source)
    lines = []
    with doc:
        for page in doc:
            words = sorted(page.get_text("words"), key=lambda w: (w[1], w[0]))
            current, row_y = [], None
            for w in words:
                x0, y0, text = w[0], w[1], w[4]
                if row_y is None or y0 - row_y > 1.5:
                    if current:
                        lines.append(" ".join(t for _, t in sorted(current)))
                    current, row_y = [], y0
                current.append((x0, text))
            if current:
                lines.append(" ".join(t for _, t in sorted(current)))
    return lines


# ---------------------------------------------------------------------------
# Extra file (customs summary table)
# ---------------------------------------------------------------------------

def _classify_extra_row(line):
    tokens = line.split()
    if not tokens:
        return None
    stars = 0
    if set(tokens[0]) == {"*"}:
        stars = len(tokens[0])
        tokens = tokens[1:]
    if "KG" not in tokens:
        return None
    kg = tokens.index("KG")
    if kg < 2 or kg + 1 >= len(tokens):
        return None
    post = tokens[kg + 1:]
    return {
        "stars": stars,
        "pre": tokens[:kg - 2],
        "gross_s": tokens[kg - 2],
        "net_s": tokens[kg - 1],
        "value_s": post[0],
        "currency": post[1] if len(post) > 1 else "",
        "tail": post[2:],
    }


def _build_extra(data_rows, group_rows, grand_rows, fmt):
    items = []
    for r in data_rows:
        pre = r["pre"]
        comm = ""
        if len(pre) > 3 and re.fullmatch(r"\d{8}", pre[3]):
            comm = pre[3]
        elif pre and re.fullmatch(r"\d{8}", pre[-1]):
            comm = pre[-1]
        items.append({
            "Customs cd": pre[0] if pre else "",
            "Bill. Doc.": pre[1] if len(pre) > 1 else "",
            "DocumentNo": pre[2] if len(pre) > 2 else "",
            "Comm. Code": comm,
            "Gross": parse_number(r["gross_s"], fmt) or 0.0,
            "Net weight": parse_number(r["net_s"], fmt) or 0.0,
            "Net Value": parse_number(r["value_s"], fmt) or 0.0,
            "Currency": r["currency"],
            "# Collies": int(r["tail"][0]) if r["tail"] and r["tail"][0].isdigit() else 0,
        })
    groups = {}
    for r in group_rows:
        cd = r["pre"][0] if r["pre"] else ""
        groups[cd] = {
            "Gross": parse_number(r["gross_s"], fmt) or 0.0,
            "Net weight": parse_number(r["net_s"], fmt) or 0.0,
            "Net Value": parse_number(r["value_s"], fmt) or 0.0,
            "Packages": int(r["tail"][0]) if r["tail"] and r["tail"][0].isdigit() else None,
        }
    grand = None
    if grand_rows:
        r = grand_rows[-1]
        grand = {
            "Gross": parse_number(r["gross_s"], fmt) or 0.0,
            "Net weight": parse_number(r["net_s"], fmt) or 0.0,
            "Net Value": parse_number(r["value_s"], fmt) or 0.0,
        }
    return items, groups, grand


def _validate_extra(items, groups, grand):
    problems = []
    for cd, sub in groups.items():
        rows = [i for i in items if i["Customs cd"] == cd]
        if not rows:
            problems.append(f"subtotal group '{cd}' has no data rows")
            continue
        for field in _WEIGHT_FIELDS:
            total = sum(r[field] for r in rows)
            if abs(total - sub[field]) > _TOLERANCE:
                problems.append(
                    f"group '{cd}' {field}: rows sum to {total:.3f}, subtotal says {sub[field]:.3f}")
    if grand:
        for field in _WEIGHT_FIELDS:
            total = sum(r[field] for r in items)
            if abs(total - grand[field]) > _TOLERANCE:
                problems.append(
                    f"grand total {field}: rows sum to {total:.3f}, document says {grand[field]:.3f}")
    elif groups:
        problems.append("no grand total (***) row found")
    return problems


def parse_extra_pdf(pdf_source):
    """
    Parse the Soudal 'extra' customs table from the PDF text layer.

    Returns {items, groups, grand_total, format, problems, annotations}
    or None when the PDF has no recognizable table text (e.g. a scan).
    """
    lines = _row_lines(pdf_source)
    if not any(line.startswith("Customs c") for line in lines):
        return None

    data_rows, group_rows, grand_rows, annotations = [], [], [], []
    for line in lines:
        row = _classify_extra_row(line)
        if row is None:
            tokens = line.split()
            if len(tokens) == 1 and tokens[0].isdigit():
                annotations.append(int(tokens[0]))
            continue
        if row["stars"] == 0:
            data_rows.append(row)
        elif row["stars"] == 2:
            group_rows.append(row)
        elif row["stars"] == 3:
            grand_rows.append(row)
        # single-star rows duplicate the data rows per billing doc — skipped

    if not data_rows:
        return None

    all_strings = []
    for r in data_rows + group_rows + grand_rows:
        all_strings += [r["gross_s"], r["net_s"], r["value_s"]]
    fmt = detect_format(all_strings, default="EU")

    items, groups, grand = _build_extra(data_rows, group_rows, grand_rows, fmt)
    problems = _validate_extra(items, groups, grand)
    if problems:
        # checksums disagree — try the opposite format before giving up
        alt = "US" if fmt == "EU" else "EU"
        alt_items, alt_groups, alt_grand = _build_extra(data_rows, group_rows, grand_rows, alt)
        alt_problems = _validate_extra(alt_items, alt_groups, alt_grand)
        if not alt_problems:
            logging.info(f"Extra table checksums resolved number format to {alt}.")
            fmt, items, groups, grand, problems = alt, alt_items, alt_groups, alt_grand, alt_problems

    return {
        "items": items,
        "groups": groups,
        "grand_total": grand,
        "format": fmt,
        "problems": problems,
        "annotations": annotations,
    }


# ---------------------------------------------------------------------------
# Factuur (invoice)
# ---------------------------------------------------------------------------

_ITEM_LINE = re.compile(r"^(\d{6})\s+([\d.,]+)\s+([A-Z]{2})\b")
_CODE_LINE = re.compile(
    r"(?:Code of goods|Code Waren)\s*:\s*(\d{8})\s+(?:Country of origin|Herkunftsland)\s*:\s*(\S+)")
_SUM_HEADER = re.compile(r"^(?:Sum by Commodity code|Zusammenfassung von Warencode)")
_SUM_ROW = re.compile(r"^(\d{8})\s+(\S+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s*$")
_TOTALS_HEADER = re.compile(r"^(?:Gross amount|Listenpreis)\b")
_TOTAL_VALUE = re.compile(r"([\d.,]+)\s+([A-Z]{3})\s*$")


def parse_factuur_text(pdf_source):
    """
    Extract exact fields from a Soudal invoice's text layer:
    Incoterm, Total Net/Gross, Total Value + Currency, commodity-code items,
    pieces per (commodity code, origin), origin declaration, pallets/drums.
    """
    lines = _row_lines(pdf_source)
    text = "\n".join(lines)
    out = {"problems": []}

    m = re.search(r"(?:Terms of delivery|Lieferbedingungen)\s+([A-Z]{3})\s*[,\s]\s*(\S[^\n]*)", text)
    if m:
        out["Incoterm"] = [m.group(1).strip(), m.group(2).strip()]

    m = re.search(r"(?:Net weight|Nettogewicht)\s+([\d.,]+)\s*KG", text)
    net_s = m.group(1) if m else None
    m = re.search(r"(?:Gross weight|Bruttogewicht)\s+([\d.,]+)\s*KG", text)
    gross_s = m.group(1) if m else None

    m = re.search(r"customs authorization No\.?\s*\(?([A-Z]{2}[A-Z0-9]+)", text, re.IGNORECASE)
    if m:
        out["Authorization"] = m.group(1).rstrip(").,")
    out["Origin Declaration"] = bool(re.search(r"preferential origin", text, re.IGNORECASE))

    pallets = [int(n) for n in re.findall(r"(?:Pallet|Palette|Drum)e?s?\s*:\s*(\d+)", text, re.IGNORECASE)]
    if pallets:
        out["Total Pallets"] = sum(pallets)

    sum_rows, pieces_raw = [], {}
    pending_qty = pending_unit = None
    total_value_s, currency = None, None
    in_sum = False
    for idx, line in enumerate(lines):
        m = _ITEM_LINE.match(line)
        if m:
            pending_qty, pending_unit = m.group(2), m.group(3)
        m = _CODE_LINE.search(line)
        if m and pending_qty is not None:
            if pending_unit in ("PC", "ST"):  # pieces / Stück
                key = (m.group(1), m.group(2))
                pieces_raw.setdefault(key, []).append(pending_qty)
            pending_qty = pending_unit = None
        if _SUM_HEADER.match(line):
            in_sum = True
            continue
        if in_sum:
            m = _SUM_ROW.match(line)
            if m:
                sum_rows.append(m.groups())
            elif sum_rows:
                in_sum = False
        if _TOTALS_HEADER.match(line):
            for nxt in lines[idx + 1: idx + 4]:
                m = _TOTAL_VALUE.search(nxt)
                if m:
                    total_value_s, currency = m.group(1), m.group(2)
                    break

    nums = [net_s, gross_s, total_value_s]
    for row in sum_rows:
        nums += [row[2], row[3], row[4]]
    for qtys in pieces_raw.values():
        nums += qtys
    fmt = detect_format([n for n in nums if n], default="EU")
    out["format"] = fmt

    if net_s:
        out["Total Net"] = parse_number(net_s, fmt)
    if gross_s:
        out["Total Gross"] = parse_number(gross_s, fmt)
    if total_value_s:
        out["Total Value"] = parse_number(total_value_s, fmt)
        out["Currency"] = currency

    items = []
    for code, origin, g_s, n_s, v_s in sum_rows:
        items.append({
            "HS Code": code,
            "COO": origin,
            "Gross Weight": parse_number(g_s, fmt) or 0.0,
            "Net Weight": parse_number(n_s, fmt) or 0.0,
            "Value": parse_number(v_s, fmt) or 0.0,
        })
    out["Items"] = items

    pieces = {}
    for key, qtys in pieces_raw.items():
        total = sum(parse_number(q, fmt) or 0.0 for q in qtys)
        if total:
            pieces[key] = int(round(total))
    out["Pieces"] = pieces

    # internal consistency: commodity items must sum to the header totals
    consistent = bool(items)
    if items and out.get("Total Net") is not None and out.get("Total Gross") is not None:
        sum_net = sum(i["Net Weight"] for i in items)
        sum_gross = sum(i["Gross Weight"] for i in items)
        if abs(sum_net - out["Total Net"]) > _TOLERANCE:
            out["problems"].append(
                f"items net weight sums to {sum_net:.3f}, invoice header says {out['Total Net']:.3f}")
            consistent = False
        if abs(sum_gross - out["Total Gross"]) > _TOLERANCE:
            out["problems"].append(
                f"items gross weight sums to {sum_gross:.3f}, invoice header says {out['Total Gross']:.3f}")
            consistent = False
    if items and out.get("Total Value") is not None:
        sum_value = sum(i["Value"] for i in items)
        if abs(sum_value - out["Total Value"]) > _TOLERANCE:
            out["problems"].append(
                f"items value sums to {sum_value:.2f}, invoice total says {out['Total Value']:.2f}")
            consistent = False
    out["items_consistent"] = consistent

    return out
