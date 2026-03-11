from io import BytesIO
import openpyxl

def write_to_excel(json_string):
    # Create a new workbook and select the active sheet
    wb = openpyxl.Workbook()
    ws = wb.active

    data = json_string

    header1 = [
        "VAT exporter",
        "EORI importer",
        "Contact",
        "Commericial reference",
        "Other ref",
        "Freight",
        "Vat cost",
        "Goods location",
        "Validation office",
        "Supplier name",
        "Street + number",
        "Postcode",
        "city",
        "Country",
        "Inco Term",
        "Place",
        "ILS number",
        "Vissel"
    ]
    
    freight_data = data.get('Freight', {})
    vat_data = data.get('Vat', {})
    
    freight_amount = freight_data.get('freight', '') if isinstance(freight_data, dict) else freight_data
    vat_amount = vat_data.get('vat', '') if isinstance(vat_data, dict) else vat_data
    
    freight_ccy = freight_data.get('currency', '') if isinstance(freight_data, dict) else ''
    vat_ccy = vat_data.get('currency', '') if isinstance(vat_data, dict) else ''

    values1 = [
        data.get('Vat Number', ''),
        data.get('Principal', '').upper(),
        data.get('Contact', '').upper(),
        data.get('container', ''),
        data.get('Other Ref', ''),
        freight_amount,
        vat_amount,
        "",
        '',
        '',
        '',
        '',
        '',
        '',
        data.get('Incoterm', '')[0] if data.get('Incoterm') is not None and isinstance(data.get('Incoterm'), list) and len(data.get('Incoterm')) > 0 else (data.get('Incoterm', '')[0] if isinstance(data.get('Incoterm'), str) and len(data.get('Incoterm')) > 0 else ''),
        data.get('Incoterm', '')[1] if data.get('Incoterm') is not None and isinstance(data.get('Incoterm'), list) and len(data.get('Incoterm')) > 1 else '',
        data.get("Customs code", ''),
        data.get("ILS_NUMBER", ''),
    ]

    values2 = [
        "",
        "",
        "",
        "",
        "",
        freight_ccy,
        vat_ccy,
        "",
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
    ]

    header2 = [
        "Commodity",
        "Description",
        "Article",
        "Collis",
        "Gross",
        "Net",
        "Origin",
        "Invoice value",
        "Currency",
        "Pieces",
        "Invoicenumber",
        "Invoice date",
        "Container",
        "Loyds",
        "Verblijfs",
        "Agent",
        "article",
        "Item",
        "BL"
    ]
    
    rows_data = []  # To store the processed rows for "items"
    row_empty = []   # To store empty values for non-"items" keys
    
    for key, value in data.items():
        # Handle array values
        if key == "items":
            for obj in value:
                mini_row = []
                
                for ordered_key in header2:
                    # Append the value in the desired order, or an empty string if the key is missing
                    if ordered_key == "Commodity":
                        mini_row.append(obj.get("HSCODE", ''))
                    elif ordered_key == "Gross":
                        mini_row.append(obj.get("Gross Weight", ''))
                    elif ordered_key == "Net":
                        mini_row.append(obj.get("Net Weight", ''))
                    elif ordered_key == "Invoice value":
                        mini_row.append(obj.get("VALEUR", ""))
                    elif ordered_key == "Origin":
                        mini_row.append(data.get("dispatch_country", ""))
                    elif ordered_key == "Collis":
                        mini_row.append(obj.get("Packages", ""))
                    elif ordered_key == "Currency":
                        mini_row.append(obj.get("DEVISES", ''))
                    else:    
                        mini_row.append(obj.get(ordered_key, ''))
                rows_data.append(mini_row)
        else:
            row_empty.append("")

    # Add keys (headers) to the first row
    ws.append(header1)

    # Add values to the second row
    ws.append(values1)
    
    # Add currencies to the cell below the values
    ws.append(values2)

    # Add empty rows and totals
    ws.append(row_empty)
    ws.append(row_empty)
    
    Total = data.get('totals', {}).get('DEVISES', 0)
    total_pallets = data.get('totals', {}).get('Packages', 0)
    total_weight_net = data.get('totals', {}).get('Net Weight', 0)
    total_weight_gross = data.get('totals', {}).get('Gross Weight', 0)

    ws.append(["Total invoices"])
    ws.append([round(Total, 2)])
    ws.append(row_empty)

    ws.append(["Total Collis"])
    ws.append([total_pallets])
    ws.append(row_empty)

    ws.append(["Total Gross", "Total Net"])
    ws.append([total_weight_gross, total_weight_net])
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