import azure.functions as func
import logging
import json
from collections import defaultdict

from alphaCargo.utils import extract_hs_code, merge_invoice_and_pl, fix_hs_codes, detect_missing_fields, repair_with_ai, get_iso_country, repair_damaged_items
from alphaCargo.functions.functions import  clean_incoterm, clean_number_from_chars, extract_and_clean, normalize_numbers, safe_float_conversion, safe_int_conversion
from alphaCargo.excel.create_excel import write_to_excel

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing file upload request.')

    try:
        req_body = req.get_json()
        files = req_body.get("files", {})
        Invs = files.get("Invs", [])
        PLs = files.get("PLs", [])
        email = req_body.get("body", "")

        all_inv_items = []
        all_pl_items = []
        
        Inv_result = {}
        PLs_result = {} 
        
        # Process each invoice file
        for file in Invs:
            documents = file.get("documents", [])
            content = file.get("content", "")
            
            file_result = {}
            file_items = []

            for page in documents:
                fields = page.get("fields", {})
                for key, value in fields.items():
                    if key in ["Items", "Summary"]:
                        arr = value.get("valueArray", [])
                        for item in arr:
                            valueObject = item.get("valueObject", {})
                            obj = {}
                            for keyObj, valObj in valueObject.items():
                                obj[keyObj] = valObj.get("content")    
                            file_items.append(obj)
                            
                    elif key == "Adress":
                        valueObject = value.get("valueObject", {})
                        row1 = valueObject.get("ROW1", {})
                        row1_val_obj = row1.get("valueObject", {})
                        obj = {}
                        for keyObj, valObj in row1_val_obj.items():
                            obj[keyObj] = valObj.get("content")    
                        file_result[key] = [obj]
                    else:
                        file_result[key] = value.get("content")      

            file_result["Items"] = file_items
            
            # Translate Country of Origin
            origin = file_result.get("Origin", "")
            if origin:
                file_result["Origin"] = get_iso_country(origin)

            # Identify if any items are damaged
            has_damaged = False
            for item in file_result.get("Items", []):
                hs = item.get("HS CODE") or item.get("Commodity")
                qty = item.get("Quantity") or item.get("Qty")
                amount = item.get("Amount") or item.get("Invoice value")
                
                # Treat empty, None, or numerical '0' as damaged
                is_qty_zero = safe_float_conversion(normalize_numbers(str(qty or "0"))) == 0
                is_amount_zero = safe_float_conversion(normalize_numbers(str(amount or "0"))) == 0

                if not hs or is_qty_zero or is_amount_zero:
                    has_damaged = True
                    break

            if has_damaged or not file_result.get("Items"):
                logging.info(f"--- DAMAGE DETECTED in Invoice ---")
                logging.info(f"Current Items: {json.dumps(file_result.get('Items', []), indent=2)}")
                logging.info(f"Triggering AI re-extraction...")
                repaired_data = repair_with_ai(content, "Invoice")
                if repaired_data and repaired_data.get("Items"):
                    logging.info("AI Repair Successful. New Items extracted.")
                    file_result["Items"] = repaired_data["Items"]
                    # Also update header fields if AI found them
                    for key in ["Invoice Number", "Inco Term", "Total Value", "Origin Country"]:
                        if not file_result.get(key) and repaired_data.get(key):
                            file_result[key] = repaired_data[key]
                else:
                    logging.error("AI Repair failed to return items.")

            # NEW: Country translation (moved after potential AI repair)
            # Check both 'Origin' (DI key) and 'Origin Country' (AI key)
            origin = file_result.get("Origin") or file_result.get("Origin Country") or ""
            file_result["Origin"] = get_iso_country(origin)
            file_result["Inco Term"] = clean_incoterm(file_result.get("Inco Term", ""))
            
            total_val = str(file_result.get("Total Value", "")).replace(' ', '')
            normalized_total = normalize_numbers(total_val)
            file_result["Total Value"] = round(safe_float_conversion(normalized_total), 2)

            for item in file_result.get("Items", []):
                price = str(item.get("Amount", "")).replace(' ', '')
                item["Amount"] = safe_float_conversion(normalize_numbers(price))
                
                hs_code = item.get("HS CODE") or item.get("Commodity") or ""
                item["HS CODE"] = extract_hs_code(str(hs_code))
                
                qty = item.get("Quantity", "") or item.get("Qty", "")
                item["Qty"] = safe_int_conversion(normalize_numbers(str(qty)))
                item["Invoice Number"] = file_result.get("Invoice Number", "")
                
            Inv_result.update(file_result)
            all_inv_items.extend(file_result.get("Items", []))
        
        Inv_result["Items"] = all_inv_items
        
        # Process each packing list file
        for file in PLs:
            documents = file.get("documents", [])
            content = file.get("content", "")

            file_result = {}
            file_items = []

            for page in documents:
                fields = page.get("fields", {})
                for key, value in fields.items():
                    if key in ["Items", "Summary"]:
                        arr = value.get("valueArray", [])
                        for item in arr:
                            valueObject = item.get("valueObject", {})
                            obj = {}
                            for keyObj, valObj in valueObject.items():
                                obj[keyObj] = valObj.get("content")    
                            file_items.append(obj)
                    elif key == "Adress":
                        valueObject = value.get("valueObject", {})
                        row1 = valueObject.get("ROW1", {})
                        row1_val_obj = row1.get("valueObject", {})
                        obj = {}
                        for keyObj, valObj in row1_val_obj.items():
                            obj[keyObj] = valObj.get("content")    
                        file_result[key] = [obj]
                    else:
                        file_result[key] = value.get("content")               

            file_result["Items"] = file_items

            # Identify if any items are damaged (PL level: NW, GW)
            has_damaged_pl = False
            for item in file_result.get("Items", []):
                nw = item.get("Net Weight") or item.get("Net")
                gw = item.get("Gross Weight") or item.get("Gross")
                
                # Treat empty or numerical '0' as damaged for PL
                is_nw_zero = safe_float_conversion(normalize_numbers(str(nw or "0"))) == 0
                is_gw_zero = safe_float_conversion(normalize_numbers(str(gw or "0"))) == 0

                if is_nw_zero or is_gw_zero:
                    has_damaged_pl = True
                    break

            if has_damaged_pl or not file_result.get("Items"):
                logging.info(f"--- DAMAGE DETECTED in Packing List ---")
                logging.info(f"Current Items: {json.dumps(file_result.get('Items', []), indent=2)}")
                logging.info(f"Triggering AI re-extraction...")
                repaired_data = repair_with_ai(content, "Packing List")
                if repaired_data and repaired_data.get("Items"):
                    logging.info("AI Repair Successful for PL.")
                    file_result["Items"] = repaired_data["Items"]
                    for key in ["Total Gross", "Total Net", "Total Packages", "Origin Country"]:
                        if not file_result.get(key) and repaired_data.get(key):
                            file_result[key] = repaired_data[key]
                else:
                    logging.error("AI Repair failed for PL.")

            # NEW: Country translation
            origin = file_result.get("Origin") or file_result.get("Origin Country") or ""
            file_result["Origin"] = get_iso_country(origin)


            # Normalize PL fields
            for key in ["Total Gross", "Total Net", "Total Packages"]:
                val = clean_number_from_chars(str(file_result.get(key, "")))
                if '.' in val or ',' in val:
                    val = normalize_numbers(val)
                
                if key == "Total Packages":
                    file_result[key] = safe_int_conversion(val)
                else:
                    file_result[key] = safe_float_conversion(val)

            for item in file_result.get("Items", []):
                item["Quantity"] = safe_int_conversion(normalize_numbers(str(item.get("Quantity", ""))))
                item["Ctns"] = safe_int_conversion(normalize_numbers(str(item.get("Ctns", ""))))
                item["Net Weight"] = safe_float_conversion(normalize_numbers(str(item.get("Net Weight", ""))))
                item["Gross Weight"] = safe_float_conversion(normalize_numbers(str(item.get("Gross Weight", ""))))
                item["Invoice Number"] = file_result.get("Invoice Number", "")

            
            PLs_result.update(file_result)
            all_pl_items.extend(file_result.get("Items", []))
        
        PLs_result["Items"] = all_pl_items

        # Fix HS codes and merge
        Inv_result = fix_hs_codes(Inv_result)
        merged_result = merge_invoice_and_pl(Inv_result, PLs_result)
        merged_result["Items"] = sorted(merged_result.get("Items", []), key=lambda x: str(x.get("HS CODE") or ""))

        
        # Email Extraction
        cleaned_email_body_html = extract_and_clean(email)
        shipping_text = merged_result.get("Origin", "")
        
        from AI_agents.OpenAI.custom_call import CustomCall
        extractor = CustomCall()
        
        prompt = f"""
You are an information extraction engine. Extract ONLY structured data from the following email and shipping text into a single plain JSON object.
- Output ONLY a single plain JSON object. No markdown.
- Numbers must be numeric. Dates must be YYYY-MM-DD.

SCHEMA:
{{
  "Client": {{ "Name": "string", "VAT": "string", "EORI": "string" }},
  "Invoice": {{ "Amount": 0.0, "Currency": "string" }},
  "Shipment": {{
    "Delivery Place": ["Name", "Street + number", "Postcode", "City", "Country Code"],
    "Reference DR": "string", "Client Reference": "string", "ETA": "YYYY-MM-DD",
    "Container Number": "string", "Container Size": "string", "Packages": 0, "Gross Weight": 0.0,
    "Origin Country": "string", "Destination Country": "string"
  }}
}}

EMAIL: {cleaned_email_body_html}
SHIPPING TEXT: {shipping_text}
"""
        email_res = extractor.send_request("System", prompt)
        if email_res:
            email_res = email_res.replace("```json", "").replace("```", "").strip()
            try:
                merged_result["Email"] = json.loads(email_res)
            except:
                merged_result["Email"] = {}
        
        # Generate Excel
        try:
            excel_file = write_to_excel(merged_result)
            
            # Safe reference generation
            email_data = merged_result.get("Email", {})
            shipment = email_data.get("Shipment", {}) if isinstance(email_data, dict) else {}
            ref_dr = shipment.get('Reference DR', 'UNKNOWN') if isinstance(shipment, dict) else 'UNKNOWN'
            container = merged_result.get('Container Number', 'UNKNOWN')
            
            filename = f"{ref_dr}-{container}.xlsx"
            headers = {
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            return func.HttpResponse(excel_file.getvalue(), headers=headers, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
        except Exception as e:
            logging.error(f"Excel Generation Error: {e}")
            return func.HttpResponse(f"Error generating excel: {e}", status_code=500)

    except Exception as e:
        logging.error(f"Global Error: {e}")
        return func.HttpResponse(body=json.dumps({"error": str(e)}), status_code=400, mimetype="application/json")
