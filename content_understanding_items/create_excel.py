from io import BytesIO

import openpyxl


def write_items_to_excel(columns, items):
    """Write the extracted goods/items as a plain table: one header row
    followed by one row per item. Returns an in-memory BytesIO xlsx."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Items"

    columns = list(columns or [])
    items = items or []

    # If no explicit column order was provided, derive it from the item keys.
    if not columns:
        seen = []
        for item in items:
            if isinstance(item, dict):
                for key in item:
                    if key not in seen:
                        seen.append(key)
        columns = seen

    ws.append(columns)

    for item in items:
        if not isinstance(item, dict):
            continue
        ws.append([item.get(column, "") for column in columns])

    # Auto-size columns for readability.
    for col in ws.columns:
        max_length = 0
        letter = col[0].column_letter
        for cell in col:
            value = "" if cell.value is None else str(cell.value)
            if len(value) > max_length:
                max_length = len(value)
        ws.column_dimensions[letter].width = max_length + 2

    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)
    return file_stream
