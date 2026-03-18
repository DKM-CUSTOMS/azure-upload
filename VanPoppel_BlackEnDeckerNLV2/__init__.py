from datetime import datetime
from AI_agents.OpenAI.custom_call import CustomCall
from ILS_NUMBER.get_ils_number import call_logic_app
import azure.functions as func
import logging
import json
import os
import openpyxl
import base64
import uuid
import re
from AI_agents.Gemeni.adress_Parser import AddressParser
from VanPoppel_BlackEnDeckerNLV2.excel.create_excel import write_to_excel

import tempfile
import time
import io
import gc

def _safe_remove(path, attempts=3, delay=0.1):
    """Try to remove a file with a few retries (useful on Windows when transient locks occur)."""
    if not path:
        return
    for i in range(attempts):
        try:
            if os.path.exists(path):
                os.remove(path)
            return
        except PermissionError as e:
            logging.warning(f"PermissionError removing file {path}, attempt {i+1}/{attempts}: {e}")
            gc.collect()
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Unexpected error removing file {path}: {e}")
            break

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Stanley Excel extraction request.')

    try:
        body = req.get_json()
        files_data = body.get("files", [])
        email_data = body.get("email", "")
        if not files_data:
            raise ValueError("No files provided in 'files' array")
    except Exception as e:
        return func.HttpResponse(
            body=json.dumps({"error": f"Invalid request format: {e}"}),
            status_code=400,
            mimetype="application/json"
        )

    result_data = None       # Will hold header/string fields from the FIRST Excel
    second_layout = False
    all_items = []           # Accumulated items from ALL Excel files
    total_value_sum = 0.0
    net_weight_sum = 0.0
    gross_weight_sum = 0.0
    collis_sum = 0.0

    for file_data in files_data:
        filename = file_data.get("filename")
        file_content_base64 = file_data.get("file")

        if not filename or not file_content_base64:
            continue

        temp_file_path = None
        workbook = None

        try:
            decoded_data = base64.b64decode(file_content_base64)
            suffix = os.path.splitext(filename)[1] or ""
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                temp_file_path = tmp.name
                tmp.write(decoded_data)

            file_extension = os.path.splitext(filename.lower())[1]

            if file_extension in [".xlsm", ".xlsx", ".xls"]:
                try:
                    workbook = openpyxl.load_workbook(temp_file_path, data_only=True)
                    sheet = workbook.active

                    def get_cell_value(col, row):
                        cell_id = f"{col}{row}"
                        return sheet[cell_id].value if sheet[cell_id] else None

                    def extract_number(value):
                        if isinstance(value, str):
                            numbers = re.findall(r"\d+", value)
                            return sum(map(int, numbers)) if numbers else None
                        if isinstance(value, (int, float)):
                            return value
                        return None

                    def find_value_by_label(label, col_label='J', col_value='K'):
                        for r in range(1, 100):
                            cell_val = get_cell_value(col_label, r)
                            if isinstance(cell_val, str) and label.lower() in cell_val.lower():
                                return get_cell_value(col_value, r)
                        return None

                    # Detect second layout (EUR1 present)
                    for row in sheet.iter_rows(values_only=True):
                        for cell in row:
                            if cell and str(cell).strip().upper() == "EUR1":
                                second_layout = True
                                break

                    # Read references and invoice numbers
                    references = []
                    invoices = []
                    ref_start_row = None
                    for r in range(1, 100):
                        val = get_cell_value('J', r)
                        if isinstance(val, str) and "Reference" in val:
                            ref_start_row = r + 1
                            break
                    if ref_start_row:
                        for r in range(ref_start_row, 100):
                            ref_val = get_cell_value('J', r)
                            inv_val = get_cell_value('K', r)
                            if ref_val is None and inv_val is None:
                                break
                            if ref_val: references.append(str(ref_val))
                            if inv_val: invoices.append(str(inv_val))

                    header_data = {
                        "reference": ", ".join(references) if references else "",
                        "invoice_number": ", ".join(invoices) if invoices else "",
                        "delivery_conditions": find_value_by_label("Incoterm", 'J', 'K'),
                        "office_of_exit": find_value_by_label("Office of Exit", 'J', 'K'),
                        "country_of_destination": find_value_by_label("Country", 'J', 'K'),
                        "total_amount": find_value_by_label("Total Amount", 'J', 'K'),
                        "currency": get_cell_value('K', 10),   # Fixed cell K10
                        "pallet_info": extract_number(find_value_by_label("Pallet", 'J', 'K')),
                        "total_gross_weight_kg": find_value_by_label("Total Gross Weight", 'J', 'K'),
                        "total_net_weight_kg": find_value_by_label("Total Net Weight", 'J', 'K'),
                        "eori": find_value_by_label("EORI", 'J', 'K'),
                        "route": find_value_by_label("Route", 'J', 'K')
                    }

                    client_data = {
                        "name": find_value_by_label("Customer", 'M', 'N'),
                        "address": find_value_by_label("Address", 'M', 'N'),
                        "postal_code_city": find_value_by_label("Postal &", 'M', 'N'),
                        "country": find_value_by_label("Country", 'M', 'N'),
                    }

                    # --- Accumulate numeric totals ---
                    try:
                        total_value_sum += float(header_data.get("total_amount") or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        net_weight_sum += float(header_data.get("total_net_weight_kg") or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        gross_weight_sum += float(header_data.get("total_gross_weight_kg") or 0)
                    except (ValueError, TypeError):
                        pass
                    try:
                        collis_sum += float(header_data.get("pallet_info") or 0)
                    except (ValueError, TypeError):
                        pass

                    # --- Parse line items ---
                    line_items = []
                    for row_num in range(2, 1000):
                        hs_code = sheet[f"B{row_num}"].value
                        if (hs_code is None or str(hs_code).strip() == "") and sheet[f"A{row_num}"].value is None:
                            break
                        if hs_code is None or str(hs_code).strip() == "":
                            continue
                        
                        origin = sheet[f"C{row_num}"].value or ""
                        amount = sheet[f"E{row_num}"].value or 0.0
                        gross = sheet[f"F{row_num}"].value or 0.0
                        net = sheet[f"G{row_num}"].value or 0.0
                        material = sheet[f"H{row_num}"].value or ""

                        eur1_cell = sheet[f"D{row_num}"]
                        eur1_flag = ""
                        if eur1_cell.fill and getattr(eur1_cell.fill, "patternType", None) in ['solid', 'gray125', 'lightDown', 'darkGrid', 'mediumGray']:
                            eur1_flag = "EUR1"
                        
                        if eur1_cell.value and "EUR1" in str(eur1_cell.value).upper():
                            eur1_flag = "EUR1"

                        try:
                            line_items.append({
                                "HSCode": str(hs_code),
                                "Origin": str(origin),
                                "Amount": float(amount) if amount else 0.0,
                                "gross_weight_kg": float(gross) if gross else 0.0,
                                "NetWeight": float(net) if net else 0.0,
                                "Description": str(material),
                                "EUR1_Flag": eur1_flag
                            })
                        except (ValueError, TypeError):
                            logging.warning(f"Skipping row {row_num} due to invalid numeric data: amount={amount}, gross={gross}, net={net}")
                            continue

                    all_items.extend(line_items)

                    # --- First Excel provides the header/string skeleton ---
                    if result_data is None:
                        full_address = ", ".join(filter(None, [
                            str(client_data["name"]),
                            str(client_data["address"]),
                            str(client_data["postal_code_city"]),
                            str(client_data["country"])
                        ]))

                        parser = AddressParser()
                        parsed_address_list = parser.parse_address(full_address)
                        parsed_address = {
                            "company_name": parsed_address_list[0] if len(parsed_address_list) > 0 else None,
                            "street": parsed_address_list[1] if len(parsed_address_list) > 1 else None,
                            "city": parsed_address_list[2] if len(parsed_address_list) > 2 else None,
                            "postal_code": parsed_address_list[3] if len(parsed_address_list) > 3 else None,
                            "country_code": parsed_address_list[4] if len(parsed_address_list) > 4 else None,
                        }

                        result_data = {
                            "ShipmentReference": header_data.get("reference"),
                            "Incoterm": (str(header_data.get("delivery_conditions") or "")) + " " + (str(parsed_address.get("city") or "")),
                            "Total Value": 0.0,       # Will be set after loop from sum
                            "NetWeight": 0.0,
                            "GrossWeight": 0.0,
                            "currency": header_data.get("currency"),
                            "Collis": 0.0,
                            "OfficeOfExit": header_data.get("office_of_exit"),
                            "PlaceOfDelivery": parsed_address,
                            "Invoice No": header_data.get("invoice_number"),
                            "Items": [],
                            "EORI": header_data.get("eori"),
                            "Route": header_data.get("route")
                        }

                        try:
                            response = call_logic_app("STANLEY", company="vp")
                            if response.get("success"):
                                result_data["ILS_NUMBER"] = response["doss_nr"]
                        except Exception as e:
                            logging.error(f"ILS_NUMBER fetch failed: {e}")

                finally:
                    if workbook is not None:
                        try:
                            workbook.close()
                        except Exception as e:
                            logging.warning(f"Error closing workbook: {e}")
                        workbook = None

        except Exception as outer_e:
            logging.error(f"Error processing file {filename}: {outer_e}", exc_info=True)

        finally:
            try:
                _safe_remove(temp_file_path)
            except Exception as e:
                logging.error(f"Failed to remove temp file {temp_file_path}: {e}")

    # --- Merge accumulated data into result_data ---
    if result_data is not None:
        result_data["Items"] = all_items
        result_data["Total Value"] = total_value_sum
        result_data["NetWeight"] = net_weight_sum
        result_data["GrossWeight"] = gross_weight_sum
        result_data["Collis"] = collis_sum

    if not result_data:
        return func.HttpResponse(
            body="No valid Excel data processed",
            status_code=400
        )

    try:
        prompt = f"""You will receive a raw email in HTML or plain text format. 
            Your task: extract the sender's email address, and return only the part before the "@" symbol.  

            For example:  
            - If the sender is "Ellen.Nowak@sbdinc.com", return "Ellen.Nowak".  
            - If the sender is "john_doe@example.org", return "john_doe".  

            Rules:  
            - Always return just the extracted string, no explanations.  
            - If no valid email is found, return an empty string.
            
            Here is the email body: '''{email_data}'''. """
        call = CustomCall()
        contact = call.send_request(role="user", prompt_text=prompt)
        
        result_data["Contact"] = contact.strip()[:10]
        excel_file_bytes = write_to_excel(result_data, second_layout)
        reference = result_data.get("ShipmentReference", f"ref-{uuid.uuid4().hex}")
        
        headers = {
            "Content-Disposition": f'attachment; filename="{reference}.xlsx"',
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
        
        body_bytes = excel_file_bytes.getvalue() if hasattr(excel_file_bytes, "getvalue") else bytes(excel_file_bytes)
        return func.HttpResponse(body_bytes, headers=headers)
    except Exception as e:
        logging.error(f"Error writing Excel: {e}", exc_info=True)
        return func.HttpResponse(body=f"Error: {e}", status_code=500)
