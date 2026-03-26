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
from VanPoppel_BlackEnDeckerNLV3.excel.create_excel import write_to_excel
from VanPoppel_BlackEnDeckerNLV3.functions.functions import extract_clean_excel_from_pdf

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
            # force GC and small sleep to allow handles to close
            gc.collect()
            time.sleep(delay)
        except Exception as e:
            logging.error(f"Unexpected error removing file {path}: {e}")
            break

def merge_pdf_results(pdf_results):
    merged = {"Items": []}
    for entry in pdf_results:
        items = entry.get("Items", [])
        if isinstance(items, list):
            merged["Items"].extend(items)
    return merged

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Stanley Excel + PDF extraction request (V3).')

    # --- Parse request body ---
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

    # Fresh containers each call (prevents stale data)
    result_data = None
    pdf_result = None
    second_layout = False
    pdf_results = []
    excel_input_rows = []  # rows from the Input sheet, used for gross_weight matching

    # --- Loop files ---
    for file_data in files_data:
        filename = file_data.get("filename")
        file_content_base64 = file_data.get("file")

        if not filename or not file_content_base64:
            logging.warning("Skipping file without filename or content.")
            continue

        temp_file_path = None
        pdf_document = None
        workbook = None

        try:
            decoded_data = base64.b64decode(file_content_base64)

            # Use a unique temporary file name (prevents collisions)
            suffix = os.path.splitext(filename)[1] or ""
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                temp_file_path = tmp.name
                tmp.write(decoded_data)

            logging.info(f"Saved temporary file {temp_file_path}")
            file_extension = os.path.splitext(filename.lower())[1]

            # --- Handle PDF ---
            if file_extension == ".pdf":
                if "eur1" not in filename.lower():
                    import fitz  # PyMuPDF
                    try:
                        pdf_document = fitz.open(temp_file_path)  # explicit open
                        text = ""
                        with fitz.open(temp_file_path) as doc:
                            for page in doc:  # iterate over all pages
                                text += page.get_text("text") + "\n"
                        pdf_result = extract_clean_excel_from_pdf(text)
                        pdf_results.append(pdf_result)
                    finally:
                        if pdf_document is not None:
                            try:
                                pdf_document.close()
                            except Exception as close_e:
                                logging.warning(f"Error closing pdf document: {close_e}")
                            pdf_document = None

            # --- Handle Excel (NLV2 dynamic label-scanning layout) ---
            elif file_extension in [".xlsm", ".xlsx", ".xls"]:
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
                        """Scan col_label for a row containing `label`, return the value
                        from col_value on that same row. If that cell is empty/None,
                        fall back to the row immediately below (K+1 pattern)."""
                        for r in range(1, 100):
                            cell_val = get_cell_value(col_label, r)
                            if isinstance(cell_val, str) and label.lower() in cell_val.lower():
                                val = get_cell_value(col_value, r)
                                if val is not None:
                                    return val
                                # fallback: value may be on the next row
                                return get_cell_value(col_value, r + 1)
                        return None

                    # Detect EUR1 layout (same as NL: scan all cells for "EUR1")
                    for row in sheet.iter_rows(values_only=True):
                        for cell in row:
                            if cell and str(cell).strip().upper() == "EUR1":
                                second_layout = True
                                break

                    # --- Read references and invoice numbers dynamically (NLV2 layout) ---
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
                        # Try several label variants for the collis/pallet count
                        "pallet_info": (
                            extract_number(find_value_by_label("Pallet", 'J', 'K'))
                            or extract_number(find_value_by_label("Collis", 'J', 'K'))
                            or extract_number(find_value_by_label("Carton", 'J', 'K'))
                            or extract_number(find_value_by_label("Cartoon", 'J', 'K'))
                            or extract_number(find_value_by_label("Box", 'J', 'K'))
                        ),
                        "total_gross_weight_kg": find_value_by_label("Total Gross Weight", 'J', 'K'),
                        "total_net_weight_kg": find_value_by_label("Total Net Weight", 'J', 'K'),
                        "eori": find_value_by_label("EORI", 'J', 'K'),
                        "route": find_value_by_label("Route", 'J', 'K'),
                    }

                    logging.info(f"header_data: {header_data}")

                    client_data = {
                        "name": find_value_by_label("Customer:", 'M', 'N'),
                        "address": find_value_by_label("Address:", 'M', 'N'),
                        "postal_code_city": find_value_by_label("Postal &", 'M', 'N'),
                        "country": find_value_by_label("Country:", 'M', 'N'),
                    }

                    # --- Parse line items (NLV2 columns: B=HS, C=Origin, D=EUR1, E=Amount, F=Gross, G=Net, H=Description) ---
                    line_items = []
                    for row_num in range(3, 1000):
                        hs_code = sheet[f"B{row_num}"].value
                        if (hs_code is None or str(hs_code).strip() == "") and sheet[f"A{row_num}"].value is None:
                            break
                        if hs_code is None or str(hs_code).strip() == "":
                            continue

                        origin   = sheet[f"C{row_num}"].value or ""
                        amount   = sheet[f"E{row_num}"].value or 0.0
                        gross    = sheet[f"F{row_num}"].value or 0.0
                        net      = sheet[f"G{row_num}"].value or 0.0
                        material = sheet[f"H{row_num}"].value or ""

                        # Detect EUR1: column D contains a value for EUR1 items
                        eur1_val  = sheet[f"D{row_num}"].value
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
                            logging.warning(f"Skipping row {row_num} due to invalid numeric data")
                            continue

                    full_address = ", ".join(filter(None, [
                        str(client_data["name"]) if client_data["name"] else "",
                        str(client_data["address"]) if client_data["address"] else "",
                        str(client_data["postal_code_city"]) if client_data["postal_code_city"] else "",
                        str(client_data["country"]) if client_data["country"] else "",
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
                        "Route": header_data.get("route"),
                    }

                    # inject ILS number
                    try:
                        response = call_logic_app("STANLEY", company="vp")
                        if response.get("success"):
                            result_data["ILS_NUMBER"] = response["doss_nr"]
                    except Exception as e:
                        logging.error(f"ILS_NUMBER fetch failed: {e}")

                    # --- Read Input sheet for gross_weight matching ---
                    # Headers in row 1, data from row 2.
                    # B=HS Codes, C=Origin, D=EUR1, E=Amount,
                    # F=Gross Weight (kg), G=Net Weight (kg), H=Material
                    try:
                        input_sheet = workbook["Input"]
                        for r in range(2, 10000):
                            hs_val = input_sheet[f"B{r}"].value
                            if hs_val is None or str(hs_val).strip() == "":
                                break
                            try:
                                excel_input_rows.append({
                                    "HSCode": str(hs_val).strip(),
                                    "Origin": str(input_sheet[f"C{r}"].value or "").strip(),
                                    "Gross":  float(input_sheet[f"F{r}"].value or 0.0),
                                    "Net":    float(input_sheet[f"G{r}"].value or 0.0),
                                    "eur1":   "N945" if (input_sheet[f"D{r}"].value not in (None, "")) else "",
                                    "_used":  False,
                                })
                            except (ValueError, TypeError):
                                continue
                        logging.info(f"Read {len(excel_input_rows)} rows from Input sheet")
                    except Exception as inp_err:
                        logging.warning(f"Could not read Input sheet: {inp_err}")

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
            # Always attempt safe remove of temp file (with retries)
            try:
                _safe_remove(temp_file_path)
            except Exception as e:
                logging.error(f"Failed to remove temp file {temp_file_path}: {e}")
    
            
    # --- Merge all PDF results (PDF items contain Description, InvoiceNumber, InvoiceDate, etc.) ---
    pdf_final_data = merge_pdf_results(pdf_results)

    # --- Merge PDF Items into Excel Skeleton ---
    # PDF items override Excel items because PDFs carry the full Description field.
    # gross_weight_kg and eur1 are injected by matching each PDF item against the
    # Input sheet rows using HSCode + Origin + NetWeight (5 % tolerance).
    if result_data and pdf_final_data and "Items" in pdf_final_data and pdf_final_data["Items"]:

        def _close_enough(a, b, tol=0.05):
            """True when a and b are within `tol` relative tolerance (or both zero)."""
            if a is None or b is None:
                return False
            if a == 0 and b == 0:
                return True
            denom = max(abs(a), abs(b))
            return denom > 0 and abs(a - b) / denom <= tol

        for pdf_item in pdf_final_data["Items"]:
            pdf_hs   = str(pdf_item.get("HSCode",    "")).strip()
            pdf_coo  = str(pdf_item.get("Origin",    "")).strip().upper()
            pdf_net  = None
            try:
                pdf_net = float(pdf_item.get("NetWeight") or 0)
            except (ValueError, TypeError):
                pass

            best = None
            # Pass 1: exact HSCode + Origin + NetWeight match (with tolerance)
            for row in excel_input_rows:
                if row["_used"]:
                    continue
                if row["HSCode"] != pdf_hs:
                    continue
                if row["Origin"].upper() != pdf_coo:
                    continue
                if _close_enough(row["Net"], pdf_net):
                    best = row
                    break

            # Pass 2: relax to HSCode + Origin only (ignore net weight)
            if best is None:
                for row in excel_input_rows:
                    if row["_used"]:
                        continue
                    if row["HSCode"] == pdf_hs and row["Origin"].upper() == pdf_coo:
                        best = row
                        break

            # Pass 3: HSCode only as last resort
            if best is None:
                for row in excel_input_rows:
                    if row["_used"]:
                        continue
                    if row["HSCode"] == pdf_hs:
                        best = row
                        break

            if best is not None:
                best["_used"] = True
                pdf_item["gross_weight_kg"] = best["Gross"]
                pdf_item["eur1"]            = best["eur1"]
            else:
                pdf_item.setdefault("gross_weight_kg", 0.0)
                pdf_item.setdefault("eur1", "")

        result_data["Items"] = pdf_final_data["Items"]
        logging.info(
            f"Final merged {len(pdf_final_data['Items'])} items from PDF "
            "(gross_weight from Input sheet)."
        )

    for item in (result_data.get("Items", []) if result_data else []):
        if "InvoiceDate" in item and isinstance(item["InvoiceDate"], str):
            raw_date = item["InvoiceDate"].strip()
            # Reformat dd-mm-yyyy or dd/mm/yyyy → yyyy-mm-dd
            date_match = re.match(r"^(\d{1,2})[-/\.](\d{1,2})[-/\.](\d{4})$", raw_date)
            if date_match:
                dd, mm, yyyy = date_match.group(1), date_match.group(2), date_match.group(3)
                item["InvoiceDate"] = f"{yyyy}-{int(mm):02d}-{int(dd):02d}"
            else:
                # Already yyyy-mm-dd or unknown format — keep as-is
                item["InvoiceDate"] = raw_date

    # --- Build Response ---
    if not result_data:
        return func.HttpResponse(
            body="No valid Excel or PDF data processed",
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
        
        # excel_file_bytes expected to be BytesIO-like
        body_bytes = excel_file_bytes.getvalue() if hasattr(excel_file_bytes, "getvalue") else bytes(excel_file_bytes)
        return func.HttpResponse(body_bytes, headers=headers)
    except Exception as e:
        logging.error(f"Error writing Excel: {e}", exc_info=True)
        return func.HttpResponse(body=f"Error: {e}", status_code=500)
