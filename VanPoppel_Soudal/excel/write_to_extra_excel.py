from io import BytesIO
import json
import logging
import openpyxl

def write_to_extra_excel(json_string):
    # Create a new workbook and select the active sheet
    wb = openpyxl.Workbook()
    ws = wb.active

    data = json_string

    header1 = [
        "VAT exporter",
        "Contact",
        "Commericial reference",
        "Other ref",
        "Freight",
        "Goods location",
        "Export office",
        "Exit office",
        "Name",
        "Street + number",
        "Postcode",
        "city",
        "Country",
        "Inco Term",
        "Place",
        "Container",
        "Truck",
        "Rex/Other",
        "Vissel",
    ]

    address = data.get('Address', [])
    name, street, city, code_postal, country = address
    if data.get('Incoterm', ['', '']) and len(data.get('Incoterm', ['', ''])) == 2:
        term, place = data.get('Incoterm', ['', ''])
    elif data.get('Incoterm', ['', '']) and len(data.get('Incoterm', ['', ''])) == 1:
        term = data.get('Incoterm', ['', ''])[0]
        place = ''
    else:
        term, place = '', ''
    
    values1 = [
        data.get('VAT', ''),
        data.get('Principal', ''),
        data.get('Reference', ''),
        data.get('Other Ref', ''),
        data.get('Freight', ''),
        data.get('kaai', ''),
        data.get('Export office', ''),
        data.get('Exit office', ''),
        name.upper() if 'name' in locals() else '',  # Safely handle variables
        street.upper() if 'street' in locals() else '',
        code_postal.upper() if 'code_postal' in locals() else '',
        city.upper() if 'city' in locals() else '',
        country.upper() if 'country' in locals() else '',
        term if 'term' in locals() else '',
        place if 'place' in locals() else '',
        data.get('Container'),
        data.get('Truck Nbr', ''),
        data.get("Customs Code", ''),
        data.get("ILS_NUMBER", ''),
    ]

    header2 = [
        "Customs code",
        "Bill. Doc.",
        "# Collies",
        "Gross",
        "Net weight",
        "Net Value",
        "Currency",
        "Comm. Code",
        "Invoice number",
        "Invoice date",
    ]
    
    rows_data = []  # To store the processed rows for "items"
    row_empty = []   # To store empty values for non-"items" keys
    
    for key, value in data.items():
        # Handle array values
        if key == "Items":
            for obj in value:
                mini_row = []
                for ordered_key in header2:
                    if ordered_key == "Invoice number":
                        mini_row.append(data.get("Inv Number", ''))
                    elif ordered_key == "Invoice date":
                        mini_row.append(data.get("Inv Date", ''))
                    elif ordered_key == "Customs code":
                        mini_row.append(obj.get("Customs cd", ''))
                    else :    
                        mini_row.append(obj.get(ordered_key, ''))
                rows_data.append(mini_row)
        else:
            row_empty.append("")

    # Add keys (headers) to the first row
    ws.append(header1)

    # Add values to the second row
    ws.append(values1)

    # Add empty rows and totals
    ws.append(row_empty)
    ws.append(row_empty)

    ws.append(["Total invoices"])
    ws.append([data.get("Total Value", 0)])
    ws.append(row_empty)

    ws.append(["Total Collis"])
    total_pallets = data.get('Total Pallets', 0)
    ws.append([total_pallets])
    ws.append(row_empty)

    ws.append(["Total Gross", "Total Net"])
    total_weight = data.get('Total Gross', 0)
    total_net = data.get('Total Net', 0)
    ws.append([total_weight, total_net])
    ws.append(row_empty)

    # Add items
    ws.append(["Items"])
    ws.append(header2)

    for arr in rows_data:
        ws.append(arr)

    # Optionally, adjust column widths for better formatting
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter  # Get the column name
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width

    # Save the workbook to a BytesIO object (in memory)
    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    return file_stream

