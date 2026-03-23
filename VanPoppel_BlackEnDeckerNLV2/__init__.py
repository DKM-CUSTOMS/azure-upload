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
from VanPoppel_BlackEnDeckerNLV2.clients.di_client import DILayoutClient
from VanPoppel_BlackEnDeckerNLV2.invoice_enricher import enrich_items

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

    result_data = None
    second_layout = False

    # ── Separate Excel and PDF files ────────────────────────────────────────
    excel_files = []
    pdf_files   = []
    for file_data in files_data:
        fname = file_data.get("filename", "").lower()
        ext   = os.path.splitext(fname)[1]
        if ext in (".pdf",):
            pdf_files.append(file_data)
        elif ext in (".xlsx", ".xlsm", ".xls"):
            excel_files.append(file_data)

    # Select which Excel file to process: prefer the one with "eur1" in filename
    selected_file = None
    for file_data in excel_files:
        if "eur1" in file_data.get("filename", "").lower():
            selected_file = file_data
            second_layout = True
            break

    if selected_file is None:
        # Fall back to first excel, then first of any file
        selected_file = (excel_files or files_data)[0]

    filename = selected_file.get("filename")
    file_content_base64 = selected_file.get("file")

    if not filename or not file_content_base64:
        return func.HttpResponse(
            body=json.dumps({"error": "Selected file has no filename or content"}),
            status_code=400,
            mimetype="application/json"
        )

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
                    "currency": find_value_by_label("Currency", 'J', 'K'),
                    "pallet_info": extract_number(find_value_by_label("Pallet", 'J', 'K')),
                    "total_gross_weight_kg": find_value_by_label("Total Gross Weight", 'J', 'K'),
                    "total_net_weight_kg": find_value_by_label("Total Net Weight", 'J', 'K'),
                    "eori": find_value_by_label("EORI", 'J', 'K'),
                    "route": find_value_by_label("Route", 'J', 'K')
                }
                
                logging.error(header_data)

                client_data = {
                    "name": find_value_by_label("Customer:", 'M', 'N'),
                    "address": find_value_by_label("Address:", 'M', 'N'),
                    "postal_code_city": find_value_by_label("Postal &", 'M', 'N'),
                    "country": find_value_by_label("Country:", 'M', 'N'),
                }

                # --- Parse line items ---
                line_items = []
                for row_num in range(3, 1000):
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

                    # Detect EUR1: column D contains a value (from formula) for EUR1 items
                    eur1_val = sheet[f"D{row_num}"].value
                    eur1_flag = eur1_val is not None and str(eur1_val).strip() != ""

                    try:
                        line_items.append({
                            "HSCode": str(hs_code),
                            "Origin": str(origin),
                            "Amount": float(amount) if amount else 0.0,
                            "gross_weight_kg": float(gross) if gross else 0.0,
                            "NetWeight": float(net) if net else 0.0,
                            "Description": str(material),
                            "eur1": "N945" if eur1_flag else ""
                        })
                    except (ValueError, TypeError):
                        logging.warning(f"Skipping row {row_num} due to invalid numeric data: amount={amount}, gross={gross}, net={net}")
                        continue

                # --- Build result_data ---
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

                def safe_float(val):
                    try:
                        return float(val or 0)
                    except (ValueError, TypeError):
                        return 0.0

                result_data = {
                    "ShipmentReference": header_data.get("reference"),
                    "Incoterm": (str(header_data.get("delivery_conditions") or "")) + " " + (str(parsed_address.get("city") or "")),
                    "Total Value": safe_float(header_data.get("total_amount")),
                    "NetWeight": safe_float(header_data.get("total_net_weight_kg")),
                    "GrossWeight": safe_float(header_data.get("total_gross_weight_kg")),
                    "currency": header_data.get("currency"),
                    "Collis": safe_float(header_data.get("pallet_info")),
                    "OfficeOfExit": header_data.get("office_of_exit"),
                    "PlaceOfDelivery": parsed_address,
                    "Invoice No": header_data.get("invoice_number"),
                    "Items": line_items,
                    "EORI": header_data.get("eori"),
                    "Route": header_data.get("route")
                }

                # ── Enrich line items with InvoiceNumber/InvoiceDate from PDF ──
                if pdf_files:
                    try:
                        logging.info(
                            f"Running DI OCR on {len(pdf_files)} PDF file(s) "
                            "to enrich invoice data"
                        )
                        di_client = DILayoutClient()
                        combined_di = {
                            "tables":           [],
                            "markdown_content": ""
                        }
                        for pdf_file_data in pdf_files:
                            pdf_b64  = pdf_file_data.get("file", "")
                            pdf_name = pdf_file_data.get("filename", "invoice.pdf")
                            if not pdf_b64:
                                continue
                            logging.info(f"Analysing PDF: {pdf_name}")
                            di_result = di_client.analyze_layout(pdf_b64)
                            combined_di["tables"].extend(di_result.get("tables", []))
                            combined_di["markdown_content"] += (
                                "\n" + di_result.get("markdown_content", "")
                            )

                        known_inv_str = header_data.get("invoice_number", "")
                        known_invoices = [
                            x.strip() for x in known_inv_str.split(",") if x.strip()
                        ]

                        enriched = enrich_items(combined_di, line_items, known_invoices)
                        line_items = enriched

                        # Log enrichment summary
                        matched       = sum(1 for r in line_items if r.get("match_status") == "matched")
                        force_matched = sum(1 for r in line_items if r.get("match_status") == "force_matched")
                        ambiguous     = sum(1 for r in line_items if r.get("match_status") == "ambiguous")
                        unresolved    = sum(1 for r in line_items if r.get("match_status") == "unresolved")
                        logging.info(
                            f"Invoice enrichment: {matched} matched, {force_matched} force_matched, "
                            f"{ambiguous} ambiguous, {unresolved} unresolved "
                            f"out of {len(line_items)} items"
                        )
                    except Exception as enrich_err:
                        logging.error(
                            f"Invoice enrichment failed: {enrich_err}",
                            exc_info=True
                        )

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
