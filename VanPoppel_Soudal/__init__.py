import datetime
import re
import azure.functions as func
import logging
import json
import os
import base64
import fitz

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.ai.formrecognizer import DocumentAnalysisClient 
from azure.core.credentials import AzureKeyCredential

# --- Imports for Splitting Logic & Your Helpers ---
from AI_agents.Mistral.MistralDocumentQA import MistralDocumentQA
from AI_agents.Gemeni.adress_Parser import AddressParser
from AI_agents.OpenAI.custom_call import CustomCall

from ILS_NUMBER.get_ils_number import call_logic_app
from VanPoppel_Soudal.excel.write_to_extra_excel import write_to_extra_excel
from VanPoppel_Soudal.helpers.functions import clean_incoterm, clean_customs_code, merge_factuur_objects
from VanPoppel_Soudal.helpers.number_format import detect_format, parse_number
from VanPoppel_Soudal.helpers.soudal_pdf import parse_extra_pdf, parse_factuur_text
from VanPoppel_Soudal.excel.create_excel import write_to_excel
from VanPoppel_Soudal.zip.create_zip import zip_excels

# --- Helper Function to Split PDF ---
def split_pdf_by_pages(source_path: str, start_pages: list) -> list:
    """
    Splits a PDF into multiple temporary files and returns their paths.
    """
    logging.info(f"⚙️ Splitting '{source_path}' into {len(start_pages)} parts...")
    temp_files = []
    temp_dir = os.path.dirname(source_path)
    base_filename = os.path.splitext(os.path.basename(source_path))[0]

    with fitz.open(source_path) as doc:
        for i, start_page in enumerate(start_pages):
            start_idx = start_page - 1
            is_last_part = (i + 1 == len(start_pages))
            end_idx = (len(doc) - 1) if is_last_part else (start_pages[i + 1] - 2)
            
            output_filename = os.path.join(temp_dir, f"{base_filename}_part_{i + 1}.pdf")
            
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=start_idx, to_page=end_idx)
            new_doc.save(output_filename)
            new_doc.close()
            
            temp_files.append(output_filename)
            logging.info(f"  -> Created temporary split file '{output_filename}'")
            
    return temp_files

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing file upload request.')

    try:
        req_body = req.get_json()
        files = req_body.get('files', [])
        email_body = req_body.get('body', [])
        subject = req_body.get('subject', '')
    except ValueError:
        return func.HttpResponse(body=json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")
    
    if not files:
        return func.HttpResponse(body=json.dumps({"error": "No selected files"}), status_code=400, mimetype="application/json")
        
    key_vault_url = "https://kv-functions-python.vault.azure.net"
    secret_name = "azure-form-recognizer-key-2" 
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    try:
        api_key = secret_client.get_secret(secret_name).value
    except Exception as e:
        return func.HttpResponse(body=json.dumps({"error": "Failed to retrieve secret", "details": str(e)}), status_code=500, mimetype="application/json")
    
    endpoint = "https://document-intelligence-python.cognitiveservices.azure.com/"
    doc_analysis_client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))
        
    factuur_results = []
    extra_file_excel_data = None
    validation_problems = []
    factuur_pieces_by_code = {}

    try:
        mistral_qa = MistralDocumentQA()
        logging.info("✅ Mistral client initialized for invoice detection.")
    except Exception as e:
        return func.HttpResponse(body=json.dumps({"error": "Failed to initialize Mistral client", "details": str(e)}), status_code=500, mimetype="application/json")

    for file_info in files:
        file_content_base64 = file_info.get('file')
        filename = file_info.get('filename', 'temp.pdf')
        
        if 'factuur' in filename.lower():
            if not file_content_base64:
                logging.warning(f"File '{filename}' has no content. Skipping.")
                continue
            
            try:
                file_content = base64.b64decode(file_content_base64)
            except Exception as e:
                logging.error(f"Failed to decode base64 for '{filename}': {e}")
                continue

            # Get number of pages in the PDF
            try:
                with fitz.open(stream=file_content, filetype="pdf") as pdf_doc:
                    num_pages = pdf_doc.page_count
                logging.error(f"The document has {num_pages if num_pages is not None else 'an unknown number of'} pages.")    
            except Exception as e:
                logging.error(f"Failed to get page count for '{filename}': {e}")
                num_pages = None

            detection_prompt = (
                "Analyze the attached document using OCR to find the starting page number of each distinct invoice."
                "A new invoice usually begins with text like 'Delivery address', 'Invoice n°', 'Sales ref', 'Cust. VAT n°' only if these text are not on the top of the page so its not a new page "
                "Return a JSON object with a single key 'invoices', which is a list of objects. "
                "Each object must have one key, 'start_page', with the page number as an integer. "
                "Example: {\"invoices\": [{\"start_page\": 1}, {\"start_page\": 3}]}. "
                "Return ONLY the valid JSON."
                "Make sure to return a logical split if there are multiple invoices. and not overlook any pages. and not gave numbers that are out of range. get the number of pages to make sure you gave the correct split. "
                f"The document has {num_pages if num_pages is not None else 'an unknown number of'} pages."
            )
        
            invoice_files_to_process = []
            original_temp_path = None
            
            try:
                logging.info(f"Asking Mistral AI to check structure of '{filename}'...")
                response_text = mistral_qa.ask_document(base64_pdf=file_content_base64, prompt=detection_prompt, filename=filename)
                
                cleaned_response = response_text.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:-3].strip()
                
                detection_result = json.loads(cleaned_response)
                logging.error(f"Detection result for '{filename}': {detection_result}")

                temp_dir = os.getenv('TEMP', '/tmp')
                original_temp_path = os.path.join(temp_dir, filename)
                with open(original_temp_path, 'wb') as f:
                    f.write(file_content)

                if "invoices" in detection_result and len(detection_result["invoices"]) > 1:
                    logging.info(f"'{filename}' contains multiple invoices. Splitting now...")
                    start_pages = [inv["start_page"] for inv in detection_result["invoices"]]
                    invoice_files_to_process = split_pdf_by_pages(original_temp_path, start_pages)
                else:
                    logging.info(f"'{filename}' contains a single invoice. Processing as is.")
                    invoice_files_to_process.append(original_temp_path)

            except Exception as e:
                logging.error(f"Failed during invoice detection for '{filename}': {e}. Processing file as a single invoice.")
                if not original_temp_path:
                    temp_dir = os.getenv('TEMP', '/tmp')
                    original_temp_path = os.path.join(temp_dir, filename)
                    with open(original_temp_path, 'wb') as f:
                        f.write(file_content)
                invoice_files_to_process.append(original_temp_path)

            for invoice_path in invoice_files_to_process:
                try:
                    with open(invoice_path, 'rb') as f:
                        invoice_bytes = f.read()

                    logging.info(f"Analyzing document part: {os.path.basename(invoice_path)}")
                    poller = doc_analysis_client.begin_analyze_document("vp-soudal-model", invoice_bytes)
                    result = poller.result()
                    
                    if not result.documents:
                        logging.warning(f"No document found in analysis result for {os.path.basename(invoice_path)}. Skipping.")
                        continue
                    
                    document = result.documents[0]
                    result_dict = {}
                    for key, value in document.fields.items():
                        if key == "Items":
                            result_dict[key] = []
                            if value.value:
                                for item in value.value:
                                    obj = {k: v.value for k, v in item.value.items()}
                                    result_dict[key].append(obj)
                        elif key in ["Total Gross", "Total Net"]:
                            result_dict[key] = value.content  
                        else:
                            result_dict[key] = value.value
                    
                    # --- Deterministic extraction from the PDF text layer (exact strings, no OCR) ---
                    text_data = {}
                    try:
                        text_data = parse_factuur_text(invoice_path)
                    except Exception as e:
                        logging.error(f"Text-layer extraction failed for {os.path.basename(invoice_path)}: {e}")
                    doc_fmt = text_data.get("format", "EU")
                    part_problems = list(text_data.get("problems", []))

                    # Incoterm: take the document's own 'Terms of delivery' line,
                    # never the delivery-address city the AI tends to pick up
                    if text_data.get("Incoterm"):
                        result_dict["Incoterm"] = text_data["Incoterm"]
                    else:
                        result_dict["Incoterm"] = clean_incoterm(result_dict.get("Incoterm", ""))
                        part_problems.append("incoterm not found in PDF text; AI value used")

                    customs_code = result_dict.get("Rex Number", "")
                    result_dict["Customs Code"] = clean_customs_code(customs_code)
                    if not result_dict["Customs Code"] and text_data.get("Authorization"):
                        result_dict["Customs Code"] = text_data["Authorization"]
                    if not text_data.get("Origin Declaration"):
                        part_problems.append("no declaration of preferential origin found in invoice text")

                    address = result_dict.get("Address", "")
                    if address:
                        # drop label lines like 'Delivery address CC8800' / 'Warenempfänger 2081279'
                        address = re.sub(r"^\s*(?:Delivery address|Warenempf\S*)\s+\S+\s*", "", str(address))
                        address = re.sub(r"Gesellschafter\s*Nr\.?\s*\d+\s*", "", address)
                    parser = AddressParser()
                    parsed_address = parser.parse_address(address)
                    if not isinstance(parsed_address, (list, tuple)) or len(parsed_address) != 5:
                        if isinstance(parsed_address, (list, tuple)):
                            parsed_address = (list(parsed_address) + [""] * 5)[:5]
                        else:
                            parsed_address = [str(parsed_address or ""), "", "", "", ""]
                    result_dict["Address"] = list(parsed_address)

                    total_gross = text_data.get("Total Gross")
                    total_net = text_data.get("Total Net")
                    result_dict["Total Gross"] = total_gross if total_gross is not None else (parse_number(result_dict.get("Total Gross"), doc_fmt) or 0.0)
                    result_dict["Total Net"] = total_net if total_net is not None else (parse_number(result_dict.get("Total Net"), doc_fmt) or 0.0)

                    invoice_value_str = result_dict.get("Total Value", "")
                    if invoice_value_str:
                        parts = str(invoice_value_str).split(" ")
                        result_dict["Currency"] = parts[1] if len(parts) > 1 else text_data.get("Currency", "")
                        result_dict["Total Value"] = parse_number(parts[0], doc_fmt) or 0.0
                    elif text_data.get("Total Value") is not None:
                        result_dict["Total Value"] = text_data["Total Value"]
                        result_dict["Currency"] = text_data.get("Currency", "")
                    else:
                        result_dict["Total Value"] = 0.00
                        result_dict["Currency"] = ""

                    if text_data.get("Total Pallets") and not result_dict.get("Total Pallets"):
                        result_dict["Total Pallets"] = text_data["Total Pallets"]
                    
                    container_number = result_dict.get("Container", "")
                    if container_number:
                        result_dict["Container"] = container_number.replace(" ", "").replace(".", "")

                    invoice_date_str = result_dict.get("Inv Date", "")
                    if invoice_date_str:
                        for fmt in ["%d.%m.%Y", "%d/%m/%Y"]:
                            try:
                                result_dict["Inv Date"] = datetime.datetime.strptime(invoice_date_str, fmt).date()
                                break
                            except ValueError:
                                continue

                    # Items: prefer the exact 'Sum by Commodity code' table from the text
                    # layer (already verified against the invoice header totals)
                    text_items = text_data.get("Items") or []
                    if text_items and text_data.get("items_consistent"):
                        raw_items = [dict(i) for i in text_items]
                    else:
                        raw_items = []
                        for item in result_dict.get("Items", []):
                            item = dict(item)
                            item["Gross Weight"] = parse_number(item.get("Gross Weight"), doc_fmt) or 0.0
                            item["Net Weight"] = parse_number(item.get("Net Weight"), doc_fmt) or 0.0
                            item["Value"] = parse_number(item.get("Value"), doc_fmt) or 0.0
                            raw_items.append(item)
                        if text_items:
                            part_problems.append("text-layer items failed consistency check; AI items used")

                    pieces_map = text_data.get("Pieces") or {}
                    cleaned_items = []
                    for item in raw_items:
                        coo = item.get("COO", "") or ""
                        gross = item.get("Gross Weight") or 0.0
                        net = item.get("Net Weight") or 0.0
                        if not coo or (gross == 0 and net == 0):
                            continue

                        # supplementary units: piece counts summed from the invoice lines
                        pieces = pieces_map.get((str(item.get("HS Code", "")), coo))
                        if pieces:
                            item["Pieces"] = pieces
                            code = str(item.get("HS Code", ""))
                            factuur_pieces_by_code[code] = factuur_pieces_by_code.get(code, 0) + pieces

                        if "(" in coo and ")" in coo:
                            item["COO"] = coo[2:].replace(" ", "").replace(")", "").replace("(", "")
                        else:
                            item["COO"] = coo.replace(" ", "")

                        item["Gross Weight"] = gross
                        item["Net Weight"] = net
                        item["Value"] = item.get("Value") or 0.0
                        item["Inv Number"] = result_dict.get("Inv Number", "")
                        item["Customs Code"] = result_dict.get("Customs Code", "")
                        item["Currency"] = result_dict.get("Currency", "")
                        cleaned_items.append(item)
                    result_dict["Items"] = cleaned_items

                    # cross-check: line items must add up to the invoice totals
                    if cleaned_items:
                        sum_net = sum(i["Net Weight"] for i in cleaned_items)
                        sum_gross = sum(i["Gross Weight"] for i in cleaned_items)
                        if result_dict["Total Net"] and abs(sum_net - result_dict["Total Net"]) > 0.02:
                            part_problems.append(f"items net weight {sum_net:.3f} != invoice total {result_dict['Total Net']:.3f}")
                        if result_dict["Total Gross"] and abs(sum_gross - result_dict["Total Gross"]) > 0.02:
                            part_problems.append(f"items gross weight {sum_gross:.3f} != invoice total {result_dict['Total Gross']:.3f}")

                    if part_problems:
                        validation_problems.extend(f"{os.path.basename(invoice_path)}: {p}" for p in part_problems)

                    doc_type = "export" if "uitvoer" in subject.lower() else "import" if "invoer" in subject.lower() else "unknown"
                    match = re.search(r'\b(uitvoer|invoer)\b[:\s]+(\d+)', subject, re.IGNORECASE)
                    reference = match.group(2) if match else None

                    result_dict["File Type"] = doc_type      
                    result_dict["Reference"] = reference
                    result_dict["Filename"] = os.path.basename(invoice_path) 
                    
                    factuur_results.append(result_dict)
                    logging.info(f"Successfully processed invoice part: {os.path.basename(invoice_path)}")

                except Exception as e:
                    logging.error(f"Error processing invoice part {os.path.basename(invoice_path)}: {e}")
                    continue

            all_temp_files = invoice_files_to_process + ([original_temp_path] if original_temp_path else [])
            for path in set(all_temp_files):
                if path and os.path.exists(path):
                    try:
                        os.remove(path)
                        logging.info(f"Cleaned up temp file: {path}")
                    except OSError as e:
                        logging.error(f"Error cleaning up temp file {path}: {e}")
            
        elif 'extra' in filename.lower():
            if not file_content_base64:
                logging.warning(f"File '{filename}' has no content. Skipping.")
                continue
            try:
                extra_file_bytes = base64.b64decode(file_content_base64)
            except Exception as e:
                logging.error(f"Failed to decode base64 for '{filename}': {e}")
                continue

            # 1) Deterministic: read the table straight from the PDF text layer and
            #    verify it against the document's own subtotal (**) and grand-total
            #    (***) rows, which also settles the EU/US number format.
            parsed_extra = None
            try:
                parsed_extra = parse_extra_pdf(extra_file_bytes)
            except Exception as e:
                logging.error(f"Text-layer parse of extra file '{filename}' failed: {e}")

            if parsed_extra and parsed_extra.get("items"):
                extra_file_excel_data = {"items": parsed_extra["items"]}
                logging.info(f"Extra file '{filename}' parsed from text layer: "
                             f"{len(parsed_extra['items'])} rows, {parsed_extra['format']} number format.")
                validation_problems.extend(f"extra {filename}: {p}" for p in parsed_extra.get("problems", []))
            else:
                # 2) Fallback for scans/unexpected layouts: AI extraction, with
                #    document-level number format detection instead of per-value guessing.
                validation_problems.append(f"extra {filename}: no readable text layer; AI extraction used (no checksum validation)")
                extra_result_dict = None
                try:
                    logging.info(f"Analyzing extra file with Azure Form Recognizer: {filename}")
                    poller = doc_analysis_client.begin_analyze_document("vp-soudalExtra-model", extra_file_bytes)
                    extra_result = poller.result()

                    if extra_result.documents:
                        extra_document = extra_result.documents[0]
                        extra_result_dict = {}

                        for key, value in extra_document.fields.items():
                            if hasattr(value, 'value') and value.value is not None:
                                if isinstance(value.value, list):
                                    extra_result_dict[key] = []
                                    for item in value.value:
                                        if hasattr(item, 'value') and isinstance(item.value, dict):
                                            obj = {k: v.value for k, v in item.value.items() if hasattr(v, 'value')}
                                            extra_result_dict[key].append(obj)
                                        else:
                                            extra_result_dict[key].append(item.value if hasattr(item, 'value') else item)
                                else:
                                    extra_result_dict[key] = value.value
                            elif hasattr(value, 'content'):
                                extra_result_dict[key] = value.content
                    else:
                        logging.warning(f"No documents found in Azure Form Recognizer result for {filename}")
                except Exception as e:
                    logging.error(f"Error during Azure Form Recognizer analysis of extra file: {e}")

                if extra_result_dict:
                    rows = extra_result_dict.get("items") or extra_result_dict.get("Items") or []
                    rows = [row for row in rows if row and isinstance(row, dict)]
                    number_strings = [str(row.get(k)) for row in rows
                                      for k in ("Gross", "Net weight", "Net Value") if row.get(k) is not None]
                    fmt = detect_format(number_strings, default="EU")
                    cleaned_rows = []
                    for row in rows:
                        if row.get('GrandTotal') == True or row.get('SubTotal') == True:
                            continue
                        if not row.get("Comm. Code") or not str(row.get("Comm. Code", "")).strip():
                            continue
                        for k in ("Gross", "Net weight", "Net Value"):
                            if k in row:
                                row[k] = parse_number(row[k], fmt) or 0.0
                        cleaned_rows.append(row)
                    extra_file_excel_data = {"items": cleaned_rows}

    if not factuur_results:
        return func.HttpResponse(body=json.dumps({"error": "No factuur files were successfully processed"}), status_code=400, mimetype="application/json")
    
    try:
        prompt = f"Extract the exit office code from the email body: '''{email_body}'''. The code is like BE212000. Return only the code or 'NOT Found', no text explanation no other text only code or 'NOT Found'."
        call = CustomCall()
        exit_office = call.send_request(role="user", prompt_text=prompt)
        
        merged_result = merge_factuur_objects(factuur_results) if len(factuur_results) > 1 else factuur_results[0]
            
        if exit_office and exit_office != "NOT Found":
            merged_result["Exit office"] = exit_office
            
        response = call_logic_app("BESOUDAL", company="vp") 
        if response.get("success"):
            merged_result["ILS_NUMBER"] = response["doss_nr"]
        else:
            logging.error(f"❌ Failed to get ILS_NUMBER: {response.get('error')}")
        
        excel_file = write_to_excel(merged_result)
        extra_file_excel = None
        if extra_file_excel_data:
            extra_result = merged_result.copy()
            extra_items = extra_file_excel_data.get("items", [])

            # supplementary units: piece counts from the invoice lines, filled only
            # when a commodity code maps to exactly one row in the extra table
            code_counts = {}
            for item in extra_items:
                code = str(item.get("Comm. Code", ""))
                code_counts[code] = code_counts.get(code, 0) + 1
            for item in extra_items:
                code = str(item.get("Comm. Code", ""))
                if code_counts.get(code) == 1 and code in factuur_pieces_by_code:
                    item.setdefault("Pieces", factuur_pieces_by_code[code])

            extra_result["Items"] = extra_items
            extra_file_excel = write_to_extra_excel(extra_result)

        reference = merged_result.get("Reference", "document")

        if extra_file_excel is not None:
            zip_file = zip_excels(None, extra_file_excel, None, f"extra_{reference}.xlsx")
        else:
            zip_file = zip_excels(excel_file, None, f"factuur_{reference}.xlsx", None)

        headers = {
            'Content-Disposition': f'attachment; filename="{reference}.zip"',
            'Content-Type': 'application/zip',
            'x-file-type': merged_result.get("File Type", "unknown"),
            'x-factuur-count': str(len(factuur_results)),
            'x-validation': 'warnings' if validation_problems else 'ok'
        }
        return func.HttpResponse(zip_file.getvalue(), headers=headers, mimetype='application/zip')
    
    except Exception as e:
        logging.error(f"Unexpected error during final processing: {e}")
        return func.HttpResponse(body=json.dumps({"error": "An unexpected error occurred during final processing", "details": str(e)}), status_code=500, mimetype="application/json")