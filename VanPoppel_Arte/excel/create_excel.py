from io import BytesIO
import logging
import openpyxl


def write_to_excel(json_string, warnings=None):
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
        "ILS NUMBER"
    ]

    headerdata = data.get('header', {} )
    footerdata = data.get('footer', {} )
    totalsdata = data.get('Totals', {} )
    email_data = data.get('email_data', {} )
    
    address = headerdata.get('address')
    if not isinstance(address, (list, tuple)) or len(address) != 5:
        if isinstance(address, (list, tuple)):
            address = (list(address) + [""] * 5)[:5]
        else:
            address = [str(address or ""), "", "", "", ""]
    name, street, city, code_postal, country = address

    incoterm_value = footerdata.get('incoterm') or ""
    if isinstance(incoterm_value, str) and incoterm_value.strip():
        incoterm_parts = incoterm_value.split(' ', 1)
        term = incoterm_parts[0]
        place = incoterm_parts[1] if len(incoterm_parts) > 1 else ''
    else:
        term, place = '', ''

    values1 = [
        data.get('Vat', ''),
        data.get('Principal', ''),
        headerdata.get('document_number', ''),
        headerdata.get('account_number', ''),
        footerdata.get('transport', ''),
        data.get('location', ''),
        data.get('Export office', ''),
        email_data.get('exit_office', ''),
        name if 'name' in locals() else '',  # Safely handle variables
        street if 'street' in locals() else '',
        code_postal if 'code_postal' in locals() else '',
        city if 'city' in locals() else '',
        country if 'country' in locals() else '',
        term if 'term' in locals() else '',
        place if 'place' in locals() else '',
        data.get('Container', ''),
        email_data.get('truck', ''),
        data.get('customs_no', ''),
        data.get('ILS_NUMBER', '')
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
        "Statistical Value",
        "Pieces",
        "Invoicenumber",
        "Invoice date",
        "Rex/other",
        "Surface"
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
                        mini_row.append(obj.get("customs_tariff", ''))
                    elif ordered_key == "Description":
                        mini_row.append(obj.get("product_name", ''))
                    elif ordered_key == "Article":
                        mini_row.append(obj.get("product_code", ''))
                    elif ordered_key == "Pieces":
                        mini_row.append((obj.get("quantity", '')))
                    elif ordered_key == "Origin":
                        mini_row.append((obj.get("origin", '')))
                    elif ordered_key == "Net":
                        mini_row.append((obj.get("net_weight", '')))
                    elif ordered_key == "Invoice value":
                        mini_row.append((obj.get("amount", '')))
                    elif ordered_key == "Currency":
                        mini_row.append(footerdata.get("currency", ''))
                    elif ordered_key == "Invoicenumber":
                        mini_row.append(obj.get("document_number", ''))
                    elif ordered_key == "Invoice date":
                        mini_row.append(obj.get("date", ''))
                    elif ordered_key == "Rex/other":
                        mini_row.append(data.get("customs_no", ''))
                    elif ordered_key == "Surface":
                        mini_row.append(obj.get("surface", ''))
                    else:    
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
    total_invoices = footerdata.get('total', 0) if footerdata.get('total', []) else 0
    ws.append([total_invoices])
    ws.append(row_empty)

    ws.append(["Total Collis"])
    total_pallets = email_data.get('colli', 0)
    ws.append([total_pallets])
    ws.append(row_empty)

    ws.append(["Total Gross", "Total Net"])
    total_Grossweight = email_data.get('gross_weight', 0)
    total_Netweight = totalsdata.get('TotalNetWeight', 0)
    ws.append([total_Grossweight, total_Netweight])
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

    # Validation warnings on a separate sheet so the main layout stays untouched
    if warnings:
        ws_warn = wb.create_sheet("Warnings")
        ws_warn.append(["Validation warnings - please double-check against the source documents"])
        for warning in warnings:
            ws_warn.append([warning])
        ws_warn.column_dimensions["A"].width = 120

    # Save the workbook to a BytesIO object (in memory)
    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    return file_stream

