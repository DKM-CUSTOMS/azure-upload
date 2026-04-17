import azure.functions as func
import logging
import json
import os
import base64
import zipfile

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.ai.formrecognizer import DocumentAnalysisClient # Use this API key to call Azure Document Intelligence
from azure.core.credentials import AzureKeyCredential

from ILS_NUMBER.get_ils_number import call_logic_app
from bleckman.helpers.functions import cast_arrays_to_float, cast_arrays_to_int, process_arrays, process_invoice_data, safe_lower
from bleckman.service.extractors import extract_text_from_first_page, extract_text_from_first_page_arrs
from bleckman.config.keywords import key_map, coordinates
from bleckman.excel.excel import create_excel

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing file upload request.')

    # Attempt to parse JSON body
    try:
        req_body = req.get_json()
        files = req_body.get('files', [])
    except ValueError:
        logging.error("Invalid JSON in request body.")
        return func.HttpResponse(
            body=json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json"
        )
    
    if not files:
        logging.warning("No files provided in the request.")
        return func.HttpResponse(
            body=json.dumps({"error": "No selected files"}),
            status_code=400,
            mimetype="application/json"
        )
        
    # Key Vault URL
    key_vault_url = "https://kv-functions-python.vault.azure.net"
    secret_name = "azure-form-recognizer-key-2"  # The name of the secret you created
    
    # Use DefaultAzureCredential for authentication
    credential = DefaultAzureCredential()

    # Create a SecretClient to interact with the Key Vault
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Retrieve the secret value
    try:
        api_key = client.get_secret(secret_name).value
    except Exception as e:
        logging.error(f"---------------Failed to retrieve secret: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": "Failed to retrieve secret", "details": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
    
    # Your Form Recognizer endpoint
    endpoint = "https://document-intelligence-python.cognitiveservices.azure.com/"

    client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(api_key))
    
    # Initialize a list to store extracted PDF data
    voorblad_data = {}
    invoices_data = []
    invoice_type = ""
    invoices_data_and_type = {}
    collis, grosses = 0.00, 0.00
    
    for file_info in files:
        file_content_base64 = file_info.get('file')
        filename = file_info.get('filename', 'temp.zip')  # Assuming zip file

        if not file_content_base64:
            logging.warning(f"File '{filename}' has no content. Skipping.")
            continue
        
        # Decode the base64-encoded content
        try:
            file_content = base64.b64decode(file_content_base64)
        except Exception as e:
            logging.error(f"Failed to decode base64 content for file '{filename}': {e}")
            return func.HttpResponse(
                body=json.dumps({"error": "Failed to decode base64 content", "details": str(e)}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Save the uploaded file temporarily
        temp_dir = os.getenv('TEMP', '/tmp')  # Use temp directory
        uploaded_file_path = os.path.join(temp_dir, filename)

        # Write the file to the temporary path
        try:
            with open(uploaded_file_path, 'wb') as temp_file:
                temp_file.write(file_content)
            logging.info(f"Saved file '{filename}' to '{uploaded_file_path}'.")
        except Exception as e:
            logging.error(f"Failed to write file '{filename}' to disk: {e}")
            return func.HttpResponse(
                body=json.dumps({"error": "Failed to write file to disk", "details": str(e)}),
                status_code=500,
                mimetype="application/json"
            )
    
        # Unzip the file and process PDFs
        try:
            with zipfile.ZipFile(uploaded_file_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)  # Extract files to temp directory

                for file_name in zip_ref.namelist():
                    logging.info(f"Found file: {file_name}")
                    # Check if the file is a PDF
                    if file_name.endswith('.pdf') or file_name.endswith('.PDF'):
                        pdf_path = os.path.join(temp_dir, file_name)
                        
                        if "Voorblad".lower() in file_name.lower():
                            logging.info(f"Processing Voorblad PDF: {file_name}")
                            # Process Voorblad PDFs (custom logic here)

                            pdf_path = os.path.join(temp_dir, file_name)    
                            with open(pdf_path, "rb") as f:
                                document = f.read()

                            poller = client.begin_analyze_document("Bleckman-voorblad-model", document)
                            result = poller.result()
                            document = result.documents
                            
                            # Initialize the result dictionary
                            result_dict = {}
                            
                            # Extract top-level fields
                            if document:
                                fields = document[0].fields
                            
                                # Add top-level fields (e.g., ID, Exit Office)
                                for key, value in fields.items():
                                    if key != "Voorblad_Items":
                                        result_dict[key] = value.value if hasattr(value, 'value') else None
                            
                                # Process the "Voorblad_Items" field
                                if "Voorblad_Items" in fields:
                                    voorblad_items = fields["Voorblad_Items"]
                            
                                    # Ensure Voorblad_Items is a dictionary
                                    if hasattr(voorblad_items, 'value') and isinstance(voorblad_items.value, dict):
                                        result_dict["Voorblad_Items"] = {}
                            
                                        # Iterate through each item in Voorblad_Items (e.g., Collis, Weight)
                                        for item_name, item_data in voorblad_items.value.items():
                                            if hasattr(item_data, 'value') and isinstance(item_data.value, dict):
                                                values = []
                            
                                                # Extract values for keys like "one", "two", "three", etc.
                                                for sub_key in ["one", "two", "three", "four", "five"]:
                                                    sub_item = item_data.value.get(sub_key, {})
                                                    values.append(sub_item.value if hasattr(sub_item, 'value') else None)
                            
                                                # Assign the extracted values to the result dictionary
                                                result_dict["Voorblad_Items"][item_name] = values
                             # Log the result_dict for debugging purposes    
                            #voorblad_data = json.loads(extract_text_from_first_page(pdf_path, coordinates, key_map))
                            #voorblad_data_items = extract_text_from_first_page_arrs(pdf_path)
                            voorblad_data = {"Reference" : result_dict.get("ID", ""), "Exit Office" : result_dict.get("Exit Office", "")}
                            voorblad_data_items = result_dict.get("Voorblad_Items", {})
                            collis, grosses = voorblad_data_items.get("Collis", {}), voorblad_data_items.get("Weight", {})
                            #voorblad_data_items = extract_text_from_first_page_arrs(pdf_path)
                            #logging.error(voorblad_data_items)
                            #collis, grosses = voorblad_data_items
                            collis, grosses = cast_arrays_to_int(collis), cast_arrays_to_float(grosses)
                            collis, grosses = process_arrays(collis, grosses)

                        elif "IF" in file_name.upper() or "TO" in file_name.upper() or "INVOICE" in file_name.upper() :
                            pdf_path = os.path.join(temp_dir, file_name)    
                            with open(pdf_path, "rb") as f:
                                document = f.read()

                            poller = client.begin_analyze_document("bleckman-modelV1", document)
                            result = poller.result()

                            document = result.documents
                            result_dict = {}
                            fields = document[0].fields
                            for key, value in fields.items():
                                if key in ["Address", "Items"]:  # Fields that contain arrays
                                    arr = value.value
                                    result_dict[key] = []
                                    for item in arr:
                                        value_object = item.value
                                        obj = {}
                                        for key_obj, value_obj in value_object.items():
                                            obj[key_obj] = value_obj.value
                                        result_dict[key].append(obj)
                                else:
                                    result_dict[key] = value.value
                            
                            logging.error(result_dict)
                            
                            if "to" in safe_lower(result_dict.get('Inv Reference', "")):
                                invoice_type = "dollar"
                                if len(result_dict.get('Items', "")) > 0 :
                                    invoices_data.append(result_dict)
                                logging.info(f"Processing TO inv: {file_name}")
                            elif "if" in safe_lower(result_dict.get('Inv Reference', "")):
                                invoice_type = "euro"
                                if len(result_dict.get('Items', "")) > 0 :
                                    invoices_data.append(result_dict)
                                logging.info(f"Processing IF inv: {file_name}")
                            else:    
                                logging.error("Invalid invoice format... Ignore File")
                            
            # Append the processed JSON to extracted_data
            invoices_data_and_type = {
                **voorblad_data,
                "invoice_type": invoice_type,
                "data": invoices_data,
                "collis": collis,
                "grosses": grosses,  
            }            
                        
        except zipfile.BadZipFile as e:
            logging.error(f"Failed to unzip file '{filename}': {e}")
            return func.HttpResponse(
                body=json.dumps({"error": "Failed to unzip file", "details": str(e)}),
                status_code=400,
                mimetype="application/json"
            )

        # Cleanup temp file
        os.remove(uploaded_file_path)
    
    result = process_invoice_data(invoices_data_and_type)

    # If the Voorblad model did not return the Bleckmann ID, fall back to the
    # merged invoice reference so the Excel and download filename still have a ref.
    if not result.get("Reference"):
        fallback_reference = result.get("inv Reference", "")
        if fallback_reference:
            result["Reference"] = fallback_reference
            logging.warning(
                "Bleckmann Reference missing from Voorblad ID; falling back to invoice reference '%s'.",
                fallback_reference
            )
    
    
    try:
        # Get the ILS number
        response = call_logic_app("BLECKMANN")

        if response["success"]:
            result["ILS_NUMBER"] = response["doss_nr"]
        else:
            logging.error(f"❌ Failed to get ILS_NUMBER: {response['error']}")
    
    except Exception as e:
        logging.exception(f"💥 Unexpected error while fetching ILS_NUMBER: {str(e)}")  
    
    # Call writeExcel to generate the Excel file in memory
    excel_file = create_excel(result)
    logging.info("Generated Excel file.")
    
    reference = result.get("Reference", "")
    reference = reference if reference else "no-ref"
    
    # Set response headers for the Excel file download
    headers = {
        'Content-Disposition': 'attachment; filename="' + reference + '.xlsx"',
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }
    # Return the Excel file as an HTTP response
    return func.HttpResponse(excel_file.getvalue(), headers=headers, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
