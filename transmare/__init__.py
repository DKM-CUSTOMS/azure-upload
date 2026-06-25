import uuid
import azure.functions as func
import logging
import json
import re

from AI_agents.OpenAI.transmare_pdf_extractor import TransmarePDFExtractor
from AI_agents.Gemeni.adress_Parser import AddressParser
from AI_agents.Gemeni.functions.functions import convert_to_list
from AI_agents.Gemeni.transmare_Email import TransmareEmailParser
from ILS_NUMBER.get_ils_number import call_logic_app
from global_db.countries.functions import get_abbreviation_by_country
from global_db.functions.dates import change_date_format
from transmare.functions.functions import  add_statistical_values, clean_incoterm, clean_Origin, clean_HS_code, clean_number_from_chars, extract_and_clean, merge_json_objects, normalize_numbers, normalize_numbers_gross, safe_float_conversion, safe_int_conversion
from transmare.excel.create_excel import write_to_excel
from global_db.functions.container import is_valid_container_number, is_valid_quay_number


class PDFClassificationError(Exception):
    def __init__(self, message, files, status_code=400):
        super().__init__(message)
        self.files = files
        self.status_code = status_code


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Transmare request.')

    try:
        req_body = req.get_json()
    except ValueError as e:
        logging.error("Invalid JSON in request body.")
        logging.error(e)
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json"
        )

    try:
        email = req_body.get("body", "")
        raw_results = extract_raw_results(req_body)

        if not raw_results:
            return func.HttpResponse(
                body=json.dumps({"error": "No invoice data extracted"}),
                status_code=400,
                mimetype="application/json"
            )

        results = [clean_invoice_result(result) for result in raw_results]
        merged_result = merge_json_objects(results)

        '''------------------   Extract data from the email   ------------------ '''
        cleaned_email_body_html = extract_and_clean(email)

        parser = TransmareEmailParser()
        parsed_result = parser.parse_email(cleaned_email_body_html)
        parsed_result = parsed_result.replace('json', '').replace('```', '').strip()
        parsed_result = convert_to_list(parsed_result)
        merged_result["Vissel"] = parsed_result.get("Vissel name")
        merged_result["Exit office"] = parsed_result.get("Exit office").replace(" ", "") if parsed_result.get("Exit office") else ""
        if not merged_result["Exit office"]:
            merged_result["Exit office"] = "BE101000"
        merged_result["kaai"] = parsed_result.get("Export kaai", "") if is_valid_quay_number(parsed_result.get("Export kaai", "")) else ""
        merged_result["Container"] = parsed_result.get("Container Number", "") if is_valid_container_number(parsed_result.get("Container Number", "")) else ""
        merged_result["Email"] = parsed_result.get("Email", "")

        prev_date = merged_result.get('Inv Date', '')
        new_date = change_date_format(prev_date)
        merged_result["Inv Date"] = new_date

        try:
            response = call_logic_app("TRANSMA")

            if response["success"]:
                merged_result["ILS_NUMBER"] = response["doss_nr"]
                logging.info(f"ILS_NUMBER: {merged_result['ILS_NUMBER']}")
            else:
                logging.error(f"Failed to get ILS_NUMBER: {response['error']}")

        except Exception as e:
            logging.exception(f"Unexpected error while fetching ILS_NUMBER: {str(e)}")

        try:
            merged_result = add_statistical_values(merged_result)
            logging.error(json.dumps(merged_result, indent=4, ensure_ascii=False))
            excel_file = write_to_excel(merged_result)
            logging.info("Generated Excel file.")

            reference = merged_result.get("Inv Reference", "") or ("transmare_" + uuid.uuid4().hex[:8])

            headers = {
                'Content-Disposition': 'attachment; filename="' + reference + '.xlsx"',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            return func.HttpResponse(excel_file.getvalue(), headers=headers, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        except Exception as e:
            logging.error(f"Error: {e}")
            return func.HttpResponse(
                f"Error processing request: {e}", status_code=500
            )

    except PDFClassificationError as e:
        logging.warning(f"Transmare PDF classification stopped processing: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": str(e), "files": e.files}),
            status_code=e.status_code,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception(f"Unexpected Transmare processing error: {e}")
        return func.HttpResponse(
            body=json.dumps({"error": "Unexpected processing error", "details": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def extract_raw_results(req_body):
    files = req_body.get("files", [])

    if files and all(isinstance(file, dict) and "documents" in file for file in files):
        return extract_results_from_di_files(files)

    pdf_entries = extract_pdf_entries(req_body)
    if not pdf_entries:
        return []

    extractor = TransmarePDFExtractor()
    results = []
    skipped = []

    for entry in pdf_entries:
        filename = entry.get("filename", "transmare.pdf")
        classification = extractor.classify_pdf(entry["file"], filename) or {
            "decision": "uncertain",
            "confidence": 0,
            "filename": filename,
            "reasons": ["classification failed"],
            "blockers": [],
        }
        decision = classification.get("decision", "uncertain")

        if decision != "invoice":
            skipped.append(classification)
            logging.info(f"Skipping PDF that is not a confirmed invoice: {filename} ({decision})")
            continue

        extracted = extractor.extract_transmare_json(entry["file"], filename)
        extracted_results = coerce_ai_extraction_to_results(extracted)
        if not extracted_results:
            skipped.append({
                "decision": "invoice_extraction_failed",
                "confidence": classification.get("confidence", 0),
                "filename": filename,
                "reasons": classification.get("reasons", []) + ["classified as invoice but extraction returned no usable invoice JSON"],
                "blockers": classification.get("blockers", []),
            })
            logging.warning(f"Invoice PDF extraction returned no usable data: {filename}")
            continue

        results.extend(extracted_results)

    if not results and skipped:
        raise PDFClassificationError(
            "No invoice PDF found in request",
            skipped,
            status_code=400,
        )

    return results


def extract_results_from_di_files(files):
    results = []

    for file in files:
        documents = file["documents"]
        result = {}

        for page in documents:
            fields = page["fields"]
            for key, value in fields.items():
                if key in ["Address", "Items"]:
                    arr = value.get("valueArray") or []
                    result[key] = []
                    for item in arr:
                        valueObject = item.get("valueObject") or {}
                        obj = {}
                        for keyObj, valueObj in valueObject.items():
                            obj[keyObj] = valueObj.get("content", "")
                        result[key].append(obj)
                else:
                    result[key] = value.get("content")

        results.append(result)

    return results


def extract_pdf_entries(req_body):
    entries = []

    if req_body.get("file"):
        entries.append({
            "file": req_body.get("file"),
            "filename": req_body.get("filename", "transmare.pdf")
        })

    for index, file_info in enumerate(req_body.get("files", [])):
        if not isinstance(file_info, dict) or "documents" in file_info:
            continue

        content = (
            file_info.get("file")
            or file_info.get("content")
            or file_info.get("base64")
            or file_info.get("data")
        )
        if content:
            entries.append({
                "file": content,
                "filename": file_info.get("filename", f"transmare_{index + 1}.pdf")
            })

    return entries


def coerce_ai_extraction_to_results(extracted):
    if not extracted:
        return []

    if isinstance(extracted, dict) and "invoices" in extracted:
        invoices = extracted.get("invoices") or []
        return [invoice for invoice in invoices if isinstance(invoice, dict)]

    if isinstance(extracted, dict) and "files" in extracted:
        return extract_results_from_di_files(extracted.get("files") or [])

    if isinstance(extracted, dict) and "Items" in extracted:
        return [extracted]

    if isinstance(extracted, list):
        return [invoice for invoice in extracted if isinstance(invoice, dict)]

    return []


def parse_int_value(value):
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    match = re.search(r"\d+(?:[.,]\d+)?", str(value or ""))
    if not match:
        return 0

    return int(parse_float_value(match.group(0)))


def parse_float_value(value):
    if isinstance(value, (int, float)):
        return float(value)

    cleaned = clean_number_from_chars(str(value or ""))
    if not cleaned:
        return 0.0

    normalized = normalize_numbers_gross(cleaned)
    if normalized is not None:
        return safe_float_conversion(normalized)

    normalized = normalize_numbers(cleaned)
    if normalized:
        return safe_float_conversion(normalized)

    return safe_float_conversion(cleaned.replace(",", "."))


def reconcile_gross_weights(result, items):
    gross_total = parse_float_value(result.get("Gross weight Total", 0))
    item_gross_values = [parse_float_value(item.get("Gross weight", 0)) for item in items]
    item_gross_sum = sum(item_gross_values)

    if len(items) == 1:
        item_gross = item_gross_values[0] if item_gross_values else 0.0
        if gross_total == 0 and item_gross > 0:
            result["Gross weight Total"] = item_gross
        elif gross_total > 0 and item_gross == 0:
            items[0]["Gross weight"] = gross_total
        else:
            result["Gross weight Total"] = gross_total
        return

    if gross_total == 0 and item_gross_sum > 0:
        result["Gross weight Total"] = item_gross_sum
    else:
        result["Gross weight Total"] = gross_total


def clean_invoice_result(result):
    result = result.copy()

    result["Incoterm"] = clean_incoterm(result.get("Incoterm", ""))

    if result.get("Vat Number", "") is not None:
        result["Vat Number"] = result.get("Vat Number", "").replace(" ", "")
    else:
        result["Vat Number"] = ""

    gross_weight_total = result.get("Gross weight Total", "")
    result["Gross weight Total"] = parse_float_value(gross_weight_total)

    address_items = result.get("Address") or [{}]
    address = address_items[0] if isinstance(address_items, list) else address_items
    parser = AddressParser()
    address = parser.format_address_to_line_old_addresses(address)
    parsed_result = parser.parse_address(address)
    result["Address"] = parsed_result

    # Always coerce summable fields to float so merging never hits str + float.
    # Empty/missing values become 0.0 (parse_float_value handles all cases).
    result["Total"] = parse_float_value(result.get("Total", 0))
    result["Freight"] = parse_float_value(result.get("Freight", 0))

    items = result.get("Items", []) or []
    totalCollis = 0
    totalNet = 0
    filtered_items = []

    for item in items:
        if not isinstance(item, dict):
            continue

        if "Article nbr" in item:
            filtered_items.append(item)
            for key, value in list(item.items()):
                if key in ["Net weight", "Gross weight", "Price", "Pieces"]:
                    if key == "Pieces":
                        pieces = parse_int_value(item.get(key, 0))
                        item[key] = pieces
                        totalCollis += pieces
                    elif key == "Gross weight":
                        item[key] = parse_float_value(item.get(key, 0.0))
                    else:
                        item[key] = parse_float_value(item.get(key, 0.0))
                        if key == "Net weight":
                            totalNet += item.get(key, 0)
                elif key == "Origin":
                    origin = clean_Origin(value)
                    item[key] = get_abbreviation_by_country(origin)
                elif key == "HS code":
                    item[key] = clean_HS_code(item.get(key, ""))

            item["Inv Reference"] = result.get("Inv Reference", "")
        else:
            logging.warning(f"Item removed because 'Article nbr' key is missing: {item}")

    reconcile_gross_weights(result, filtered_items)

    result["Items"] = filtered_items
    result["Total pallets"] = totalCollis
    result["Total net"] = totalNet

    return result
