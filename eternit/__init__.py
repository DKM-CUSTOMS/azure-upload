import azure.functions as func
import logging
import json
from collections import defaultdict

from AI_agents.Gemeni.adress_Parser import AddressParser
from AI_agents.OpenAI.custom_call import CustomCall
from ILS_NUMBER.get_ils_number import call_logic_app
from global_db.countries.functions import get_abbreviation_by_country
from eternit.functions.functions import add_pieces_to_hs_and_totals, clean_customs_code, clean_incoterm, clean_number_from_chars, extract_and_clean, extract_customs_code, extract_data, merge_json_objects, normalize_numbers, safe_float_conversion, safe_int_conversion
from eternit.excel.create_excel import write_to_excel

def fall_back(table_data):
    """Extract and log minimal data from customs tariff table"""
    try:
        simplified = []
        current_row = {}
        
        for cell in table_data.get("cells", []):
            row_idx = cell.get("rowIndex")
            content = cell.get("content", "")
            
            if row_idx == 0:  # Skip header row
                continue
                
            if row_idx not in [r["row"] for r in simplified if "row" in r]:
                current_row = {"row": row_idx}
                simplified.append(current_row)
            
            # Log to see column mapping
            col_idx = cell.get("columnIndex")
            logging.error(f"Row {row_idx}, Col {col_idx}: {content}")
            
            # Map column indices to fields based on your data
            if col_idx == 0:
                # Column 0: Contains both C.T and customs_tariff (e.g., "01 68118200")
                if " " in content:
                    parts = content.split(" ", 1)
                    current_row["C.T"] = parts[0]
                    current_row["customs_tariff"] = parts[1]
                else:
                    current_row["C.T"] = content
            elif col_idx == 1:
                # Column 1: Gross Weight (without KG)
                current_row["gross_weight"] = content
            elif col_idx == 2:
                # Column 2: Net Weight (with KG)
                current_row["nett_weight"] = content
            elif col_idx == 3:
                # Column 3: Net Value
                current_row["net_value"] = content
        
        logging.error("=== FALLBACK TABLE DATA ===")
        logging.error(json.dumps(simplified, indent=2))
        return simplified
        
    except Exception as e:
        logging.error(f"Fallback function error: {e}")
        return []

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing file upload request.')

    # Attempt to parse JSON body
    try:
        req_body = req.get_json()
        files = req_body.get("files", "")
        email = req_body.get("body", "")

        fallback_data = []  # Changed to list to collect all fallback data
        
        # Find and store all fallback table data
        if files:
            for file_idx, file in enumerate(files):
                if "tables" in file:
                    for table in file["tables"]:
                        table_str = json.dumps(table)
                        if '"content": "Customs tariff"' in table_str:
                            table_data = fall_back(table)
                            if table_data:  # Only add if not None/empty
                                fallback_data.extend(table_data)  # Add to list instead of overwrite
                
                if "documents" in file:
                    for doc in file["documents"]:
                        if "tables" in doc:
                            for table in doc["tables"]:
                                table_str = json.dumps(table)
                                if '"content": "Customs tariff"' in table_str:
                                    table_data = fall_back(table)
                                    if table_data:  # Only add if not None/empty
                                        fallback_data.extend(table_data)  # Add to list instead of overwrite

        resutls = []
        
        for file in files :
            documents = file.get("documents")

            result = {}

            for page in documents:
                fields = page.get("fields")
                for key, value in fields.items():
                    if key in ["Items", "Summary"]:
                        arr = value.get("valueArray")
                        result[key] = []
                        if arr is None:
                            continue
                        for item in arr:
                            valueObject = item.get("valueObject")
                            obj = {}
                            for keyObj, valueObj in valueObject.items():
                                obj[keyObj] = valueObj.get("content")    
                            result[key].append(obj)
                            
                    elif key == "Adress":
                        result[key] = []
                        valueObject = value.get("valueObject")
                        arr = valueObject.get("ROW1")
                        valueObject = arr.get("valueObject")
                        obj = {}
                        for keyObj, valueObj in valueObject.items():
                            obj[keyObj] = valueObj.get("content")    
                        result[key].append(obj)
                    else :
                        result[key] = value.get("content")      

            '''------------------   Clean the JSON response   ------------------ '''
            # Log original summary before fallback
            logging.error("=== SUMMARY BEFORE FALLBACK ===")
            logging.error(json.dumps(result.get("Summary"), indent=2))
            
            # Check if Summary has any missing or invalid values, use fallback data
            if fallback_data and (not result.get("Summary") or 
                any(
                    item.get("HS") in [None, "", 0] or
                    item.get("Gross Weight") in [None, "", 0] or
                    item.get("Net Weight") in [None, "", 0] or
                    item.get("Value") in [None, "", 0]
                    for item in result.get("Summary", [])
                )):
                
                logging.error("=== USING FALLBACK DATA FOR SUMMARY (missing/invalid fields) ===")
                
                # Create a dictionary of fallback items keyed by C.T + HS to avoid duplicates
                fallback_dict = {}
                for item in fallback_data:
                    key = f"{item.get('C.T', '')}_{item.get('customs_tariff', '')}"
                    
                    # Clean the weights by removing "KG" if present
                    gross_weight = item.get("gross_weight", "0").replace(" KG", "").strip()
                    nett_weight = item.get("nett_weight", "0").replace(" KG", "").strip()
                    net_value = item.get("net_value", "0").strip()
                    
                    # Normalize the numbers (convert European format to proper number string)
                    gross_weight = normalize_numbers(gross_weight)
                    nett_weight = normalize_numbers(nett_weight)
                    net_value = normalize_numbers(net_value)
                    
                    fallback_dict[key] = {
                        "C.T": item.get("C.T", ""),
                        "HS": item.get("customs_tariff", ""),
                        "Gross Weight": safe_float_conversion(gross_weight),
                        "Net Weight": safe_float_conversion(nett_weight),
                        "Value": safe_float_conversion(net_value)
                    }
                
                # Convert back to list
                result["Summary"] = list(fallback_dict.values())
                
                # Log summary after fallback
                logging.error("=== SUMMARY AFTER FALLBACK ===")
                logging.error(json.dumps(result.get("Summary"), indent=2))

            #clean and split the incoterm
            result["Incoterm"] = clean_incoterm(result.get("Incoterm", ""))
            first_twoChars = result.get("Reference", "")[:2]
            if first_twoChars == "00":
                result["Reference"] = result.get("Reference", "")[2:]
            
            #clean and convert the Gross weight
            gross_weight_total = result.get("Gross weight Total", "")
            gross_weight_total = clean_number_from_chars(gross_weight_total)
            if '.' in gross_weight_total or ',' in gross_weight_total:
                gross_weight_total = normalize_numbers(gross_weight_total)
            result["Gross weight Total"] = safe_float_conversion(gross_weight_total)

            #clean and convert the Gross weight
            gross_weight_total = result.get("Freight", "")
            gross_weight_total = clean_number_from_chars(gross_weight_total)
            if '.' in gross_weight_total or ',' in gross_weight_total:
                gross_weight_total = normalize_numbers(gross_weight_total)
            result["Freight"] = safe_float_conversion(gross_weight_total)
            
            #clean the customs code
            customs_code = result.get("Customs code", "") if result.get("Customs code", "") else ""
            del result["Customs code"]
            result["Customs Code"] = clean_customs_code(customs_code)

            #switch the address country to abbr
            address = result.get("Adress", "")[0]
            parser = AddressParser()
            address = parser.format_address_to_line_old_addresses(address)
            parsed_result = parser.parse_address(address)
            result["Adress"] = parsed_result
            #address["Country"] = get_abbreviation_by_country(address["Country"])

            #clean and split the total value
            total = result.get("Total", "")
            value = normalize_numbers(total)
            value = safe_float_conversion(value)
            total = round(value, 2)
            result["Total"] = total

            #update the numbers in the items
            items = result.get("Items", "")  
            for item in items :
                #handle the value
                Price = item.get("Value", "")
                Price = normalize_numbers(Price)
                Price = safe_float_conversion(Price)
                item["Value"] = Price
                
                #handle the Qty
                Qty = item.get("Qty", "")
                Qty = normalize_numbers(Qty)
                Qty = safe_int_conversion(Qty)
                item["Qty"] = Qty
                
                #handle the Pcs
                Pcs = item.get("Pcs", "")
                if not Pcs:
                    Pcs = "0"
                Pcs = Pcs.replace(",", "")
                Pcs = safe_int_conversion(Pcs)
                item["Pcs"] = Pcs
                
                #handle the country
                Country = item.get("Origin", "")
                if Country:
                    Country = get_abbreviation_by_country(Country)
                    item["Origin"] = Country

                item["Inv Reference"] = result.get("Inv Reference", "")
            
            merged = defaultdict(lambda: {
                "Qty": 0,
                "Pcs": 0,
                "Inv Reference": "",
                "Origin": ""
            })

            for item in items:
                ct = item['C.T']
                merged[ct]["Qty"] += item.get("Qty", 0)
                merged[ct]["Pcs"] += item.get("Pcs", 0)
                if not merged[ct]["Inv Reference"]:
                    merged[ct]["Inv Reference"] = item.get("Inv Reference", "")
                if not merged[ct]["Origin"]:
                    merged[ct]["Origin"] = item.get("Origin", "")

            # Final output
            merged_list = [
                {
                    "C.T": ct,
                    "Qty": values["Qty"],
                    "Pcs": values["Pcs"],
                    "Inv Reference": values["Inv Reference"],
                    "Origin": values["Origin"]
                }
                for ct, values in merged.items()
            ]
            
            result["Items"] = merged_list
            
            # Convert items to a dict for quick access by C.T
            items_dict = {item['C.T']: item for item in result['Items']}

            # Merge 'Origin' into Summary based on C.T
            for summary in result['Summary']:
                ct = summary['C.T']
                if ct in items_dict:
                    summary['Origin'] = items_dict[ct].get('Origin', '')
                    summary['Qty'] = items_dict[ct].get('Qty', '')
                    summary['Pcs'] = items_dict[ct].get('Pcs', '')
                    summary['Inv Reference'] = items_dict[ct].get('Inv Reference', '')
            
            del result['Items']
            
            #update the numbers in the items
            summaries = result.get("Summary", "")
            TotalNetWeight = 0
            for summary in summaries :
                #handle the value
                Price = summary.get("Value")
                if Price is None:
                    Price = ""
                elif isinstance(Price, str):
                    Price = normalize_numbers(Price)
                Price = safe_float_conversion(Price)
                summary["Value"] = Price
                
                #handle the value
                GrossWeight = summary.get("Gross Weight")
                if GrossWeight is None:
                    GrossWeight = ""
                elif isinstance(GrossWeight, str):
                    GrossWeight = normalize_numbers(GrossWeight)
                GrossWeight = safe_float_conversion(GrossWeight)
                summary["Gross Weight"] = GrossWeight
                
                #handle the value
                NetWeight = summary.get("Net Weight")
                if NetWeight is None:
                    NetWeight = ""
                elif isinstance(NetWeight, str):
                    NetWeight = normalize_numbers(NetWeight)
                NetWeight = safe_float_conversion(NetWeight)
                TotalNetWeight += NetWeight
                summary["Net Weight"] = NetWeight

            result["Net weight Total"] = TotalNetWeight
            resutls.append(result)
            
        # Merge JSON objects
        merged_result = merge_json_objects(resutls)
        

        '''------------------   Extract data from the email   ------------------ '''    
        #Extract the body data
        cleaned_email_body_html = extract_and_clean(email)
        
        #extract the table as json object
        from AI_agents.OpenAI.custom_call import CustomCall

        extractor = CustomCall()
        prompt = f"""
        Extract the following fields from the email below:

        1. Exit office name (from lines like 'Port of EXIT')
        2. Total pallets (as a number from lines like 'Pallets')
        3. Freight cost (as a float from lines like 'Vrachtkost', ignore currency symbols)

        Only return a JSON object with these keys:
        - exit_office
        - collis
        - freight_cost

        Email:
        {cleaned_email_body_html}
        """

        role = "You are an expert in parsing informal shipping-related emails. Focus on extracting specific structured fields only and return clean values ready for processing (e.g., no currency signs, no extra words)."

        result = extractor.send_request(role, prompt)
        result = result.replace("```", "").replace("json", "").strip()
        result = json.loads(result)
        
        merged_result["Exit office"] = result.get("exit_office", "")
        merged_result["Total pallets"] = result.get("collis", 0)
        merged_result["Freight"] = result.get("freight_cost", 0.0)
        
        if merged_result.get("Summary", ""):
            # Extract and clean Customs Tariff Codes in Summary
            for item in merged_result.get("Summary", []):
                if item.get("HS") is None:
                    item["HS"] = ""
        
        if len(merged_result.get("Summary", [])) > 1:
            # Sort items by Customs Tariff Code
            sorted_items = sorted(merged_result.get("Summary"), key=lambda x: x.get("HS", ""))
            merged_result["Summary"] = sorted_items  
        
        # Log final summary
        logging.error("=== FINAL SUMMARY ===")
        logging.error(json.dumps(merged_result.get("Summary"), indent=2))
        
        try:
            # Get the ILS number
            response = call_logic_app("ETERNIT")

            if response["success"]:
                merged_result["ILS_NUMBER"] = response["doss_nr"]
                logging.info(f"ILS_NUMBER: {merged_result['ILS_NUMBER']}")
            else:
                logging.error(f"Failed to get ILS_NUMBER: {response['error']}")
                
        except Exception as e:
            logging.exception(f"Unexpected error while fetching ILS_NUMBER: {str(e)}")

        try:
            # Call writeExcel to generate the Excel file in memory
            excel_file = write_to_excel(merged_result)
            logging.info("Generated Excel file.")
            
            reference = merged_result.get("Inv Reference", "")

            # Set response headers for the Excel file download
            headers = {
                'Content-Disposition': 'attachment; filename="' + reference + '.xlsx"',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            # Return the Excel file as an HTTP response
            return func.HttpResponse(excel_file.getvalue(), headers=headers, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
        except Exception as e:
            logging.error(f"Error: {e}")
            return func.HttpResponse(
                f"Error processing request: {e}", status_code=500
            )              

    except ValueError as e:
        logging.error("Invalid JSON in request body.")
        logging.error(e)
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json"
        )