"""
Test script to verify EUR1 color detection in column D of Stanley Excel files.
Place the Excel file (.xlsx/.xlsm) in the project root and run this script.
"""

import glob
import json
import os
import openpyxl


def is_visibly_filled(cell):
    """
    Return True if the cell has a real stored fill style.
    Works for manually filled cells.
    Does NOT evaluate Excel conditional formatting results.
    """
    fill = cell.fill

    if fill is None:
        return False

    pattern = fill.patternType

    # Default empty cells usually have patternType = None
    # Only treat actual fill patterns as colored
    if pattern in (None, "none"):
        return False

    fg = fill.fgColor
    bg = fill.bgColor

    # Solid fill is the most common for highlighted cells
    if pattern == "solid":
        if fg is None:
            return False

        # rgb color
        if fg.type == "rgb" and fg.rgb not in (None, "00000000", "000000", "000000FF"):
            return True

        # theme or indexed can also be a real visible fill
        if fg.type in ("theme", "indexed"):
            return True

        return False

    # Other pattern fills also count as styled/colored
    return True


def get_fill_info(cell):
    """Useful debug snapshot for a cell fill."""
    fill = cell.fill
    fg = fill.fgColor if fill else None
    bg = fill.bgColor if fill else None

    def safe_attr(obj, attr):
        try:
            return getattr(obj, attr, None)
        except Exception:
            return None

    return {
        "patternType": safe_attr(fill, "patternType"),
        "fg_type": safe_attr(fg, "type"),
        "fg_rgb": safe_attr(fg, "rgb"),
        "fg_theme": safe_attr(fg, "theme"),
        "fg_indexed": safe_attr(fg, "indexed"),
        "bg_type": safe_attr(bg, "type"),
        "bg_rgb": safe_attr(bg, "rgb"),
        "bg_theme": safe_attr(bg, "theme"),
        "bg_indexed": safe_attr(bg, "indexed"),
    }


def extract_items(filepath):
    """Extract line items from a Stanley Excel file, detecting EUR1 via colored column D."""
    workbook = openpyxl.load_workbook(filepath, data_only=True)
    sheet = workbook.active

    line_items = []

    for row_num in range(3, 1000):
        a_val = sheet[f"A{row_num}"].value
        hs_code = sheet[f"B{row_num}"].value

        # Stop only after a truly empty row
        if (a_val is None or str(a_val).strip() == "") and (hs_code is None or str(hs_code).strip() == ""):
            break

        # Skip rows without HS code
        if hs_code is None or str(hs_code).strip() == "":
            continue

        origin = sheet[f"C{row_num}"].value or ""
        amount = sheet[f"E{row_num}"].value or 0.0
        gross = sheet[f"F{row_num}"].value or 0.0
        net = sheet[f"G{row_num}"].value or 0.0
        material = sheet[f"H{row_num}"].value or ""

        # Detect EUR1: column D contains a value (from formula) for EUR1 items
        eur1_val = sheet[f"D{row_num}"].value
        eur1_flag = eur1_val is not None and str(eur1_val).strip() != ""

        print(
            f"Row {row_num:3d} | "
            f"D value: {repr(eur1_val):12} | "
            f"EUR1: {eur1_flag}"
        )

        line_items.append({
            "row": row_num,
            "HSCode": str(hs_code),
            "Origin": str(origin),
            "Amount": float(amount) if amount not in (None, "") else 0.0,
            "gross_weight_kg": float(gross) if gross not in (None, "") else 0.0,
            "NetWeight": float(net) if net not in (None, "") else 0.0,
            "Description": str(material),
            "eur1": "N945" if eur1_flag else ""
        })

    workbook.close()
    return line_items


if __name__ == "__main__":
    root = os.path.dirname(os.path.abspath(__file__))
    files = glob.glob(os.path.join(root, "*.xlsx")) + glob.glob(os.path.join(root, "*.xlsm"))

    if not files:
        print("No .xlsx or .xlsm files found in project root.")
        print(f"Place your Excel file in: {root}")
    else:
        for fp in files:
            print(f"\nProcessing: {os.path.basename(fp)}")
            items = extract_items(fp)
            print(json.dumps(items, indent=2))