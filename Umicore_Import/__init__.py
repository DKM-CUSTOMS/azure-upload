import datetime
import azure.functions as func
import logging
import json
import base64
from openai import AsyncOpenAI
import fitz  # PyMuPDF
from io import BytesIO
from azure.identity.aio import DefaultAzureCredential
from azure.keyvault.secrets.aio import SecretClient
import asyncio
import time
import re
from contextlib import asynccontextmanager
import uuid

from ILS_NUMBER.get_ils_number import call_logic_app
from Umicore_Import.helpers.functions import merge_into_items, split_cost_centers, transform_afschrijfgegevens, transform_inklaringsdocument
from Umicore_Import.excel.create_excel import write_to_excel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
KEY_VAULT_URL = "https://kv-functions-python.vault.azure.net"
OPENAI_SECRET_NAME = "OPENAI-API-KEY"
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_CONCURRENCY = 3  # Maximum number of concurrent API calls

def clean_c670(value):
    """Return only the C670 code, stripping any leading 'Bijlage' annex prefix.

    The model occasionally returns e.g. 'Bijlage VII-C672' instead of 'VII-C672'.
    """
    if not isinstance(value, str):
        return value
    # Remove a leading 'Bijlage' (case-insensitive) and any following separators/spaces
    cleaned = re.sub(r'^\s*bijlage\b[\s:.-]*', '', value, flags=re.IGNORECASE)
    return cleaned.strip()

def is_pdf(filename):
    """Check if file is PDF based on extension"""
    return filename.lower().endswith('.pdf')

def check_filename_pattern(filename):
    """Check if filename contains required patterns"""
    filename_lower = filename.lower()
    if "inklaringsdocument" in filename_lower:
        return "inklaringsdocument"
    elif "afschrijfgegevens" in filename_lower:
        return "afschrijfgegevens"
    return None

@asynccontextmanager
async def get_openai_client():
    """Context manager to safely create and close OpenAI client"""
    client = None
    credential = None
    
    try:
        # Create credential and secret client
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        
        try:
            # Get API key
            secret = await secret_client.get_secret(OPENAI_SECRET_NAME)
            api_key = secret.value
            
            # Create OpenAI client
            client = AsyncOpenAI(api_key=api_key)
            yield client
            
        finally:
            # Close secret client
            await secret_client.close()
            
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {str(e)}")
        raise
    finally:
        # Ensure credential is closed
        if credential:
            await credential.close()
        
        # Ensure client is closed
        if client:
            await client.close()

@asynccontextmanager
async def open_pdf(pdf_content):
    """Context manager to safely open and close PDF document"""
    doc = None
    try:
        pdf_file = BytesIO(pdf_content)
        doc = fitz.open(stream=pdf_file, filetype="pdf")
        yield doc
    finally:
        if doc:
            doc.close()

async def retry_async(func, *args, max_attempts=MAX_RETRY_ATTEMPTS, **kwargs):
    """Retry an async function with exponential backoff"""
    last_exception = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt < max_attempts:
                wait_time = RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                logger.warning(f"Attempt {attempt} failed with error: {str(e)}. Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"All {max_attempts} attempts failed. Last error: {str(e)}")
                raise last_exception

async def process_page_with_openai_a(client, text_content, page_num, total_pages):
    """Process a single page with OpenAI API for inklaringsdocument"""
    logger.info(f"Processing inklaringsdocument page {page_num + 1}/{total_pages}")
    
    try:
        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Extract specific information from customs documents and return as JSON."},
                {"role": "user", "content": f"""Extract the following details from the document and return them as a structured JSON object: 
                {text_content}

                - **VAT Importer**: Extract as a continuous number string without dots or spaces (e.g., "BE0796615072").  
                - **EORI Importer**: Extract as is (e.g., "BECN000016").  
                - **Commercial Reference**: Extract the shipment number (e.g., "51033084").  
                - **Other Reference**: Extract the contract number (e.g., "136818003").  
                - **Incoterm**: Extract the incoterm as uppercase (e.g., "DPU").  
                - **Place**: Extract and return only the city name in uppercase (e.g., "ANTWERP").  
                - **Entrepot**: Extract the reference number for the customs warehouse (e.g., "U-00160-A").  
                - **Commodity**: Extract the goods code (e.g., "8549210000").  
                - **Description**: Extract the first two words of the goods description (e.g., "Elektrisch en elektronische").  
                - **Origin**: Extract the country name and convert it to a 2-letter country code (e.g., "MY" instead of "Malaysia").  
                - **Invoice Value**: Extract and return as a float value (e.g., `1315555.87`).  
                - **Currency**: Extract and return as is (e.g., "EUR").
                - **License**: Extract and return as is (e.g., "ET90.500/12").
                - **Vak 24**: Extract and return as is (e.g., "41").
                - **Vak 37**: Extract and return as is (e.g., "4500").
                - **Vak 44**: Extract and return as is (e.g., "BEVALA00011").
                - **cost center**: Extract and return as is (e.g., "HBN5046").
                - **C670**: Extract the code found after "Afvalstoffenreglementering" at the bottom of the page. Only return the code or number itself, WITHOUT any leading word like "Bijlage" (e.g., return "VII-C672", not "Bijlage VII-C672"). Examples: "BE001014800", "VII-C672", "CA715086", "OVAM-Y923". If "GEEN" is found, return "Geen".
                

                ### **JSON Output Format:**  
                Return the extracted data in the following format:  
                ```json
                {{
                  "vat_importer": ,
                  "eori_importer": ,
                  "commercial_reference": ,
                  "other_reference": ,
                  "incoterm": ,
                  "place": ,
                  "entrepot": ,
                  "commodity": ,
                  "description": ,
                  "origin":,
                  "invoice_value": ,
                  "currency": ,
                  "License" : ,
                  "Vak 24" : ,
                  "Vak 37" : ,
                  "Vak 44" : ,
                  "cost center" : ,
                  "C670" : 
                }}```"""}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        result = response.choices[0].message.content
        
        # Extract JSON content between triple backticks if present
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', result)
        if json_match:
            result = json_match.group(1)
        
        # Parse the JSON content
        page_data = json.loads(result)

        # Normalize C670: keep only the code, drop any 'Bijlage' prefix
        if isinstance(page_data, dict) and "C670" in page_data:
            page_data["C670"] = clean_c670(page_data["C670"])

        logger.info(f"Inklaringsdocument page {page_num + 1} extracted: {json.dumps(page_data, indent=2)}")

        return {
            "page_number": page_num + 1,
            "extracted_data": page_data
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error on page {page_num + 1}: {str(e)}")
        logger.error(f"Raw content: {result}")
        return {
            "page_number": page_num + 1,
            "error": f"JSON parsing error: {str(e)}",
            "raw_content": result
        }
    except Exception as e:
        logger.error(f"Error processing page {page_num + 1}: {str(e)}")
        return {
            "page_number": page_num + 1,
            "error": str(e)
        }

async def process_page_with_openai_b(client, text_content, page_num, total_pages):
    """Process a single page with OpenAI API for afschrijfgegevens"""
    logger.info(f"Processing afschrijfgegevens page {page_num + 1}/{total_pages}")
    
    try:
        prompt = f"""Extract the following details from the document and return them as a structured JSON object:

{text_content}

Kaai: Extract the value after "Kaai:" in uppercase format (e.g., "KAAI 1700").
Agent: Extract the exact value after "Agent:" (e.g., "MSCBEL").
Lloydsnummer: Extract the numerical value after "Lloydsnummer:" (e.g., "9839272").
Verblijfsnummer: Extract the numerical value after "Verblijfsnummer:" (e.g., "293278").
BL: Extract the BL number after "BL:" and return it in uppercase (e.g., "MEDUPQ337602").
Artikel Nummer: Extract the numeric value after "Artikel nummer:" as string (e.g., "0040").
Item: Extract the value following "Item:" (e.g., "001").

Cost Centers: A list of extracted KP sections, each containing relevant details.
For Each Cost Center (KP Section):
For every section starting with KP:, extract:
- KP: The cost center code (e.g., "HBN5101").
- Invoice Number: The numerical value following the KP code on the same line (e.g., "137622000").
- Contract number : The contract number that follows the KP code (e.g., "HBN5101").
- Company: The company name that follows the cost center information (e.g., "YOKOHAMA METAL CO LTD").
- Description: The text describing the goods, which follows the company name (e.g., "SWEEPS FROM MIXED ELECTRONIC COMPONENTS").
- Items: A list containing container-specific data.

For Each Container Under a KP:
Extract the container data and ensure proper formatting:
- Container: Extract the container number, remove spaces and special characters (e.g., "TGBU 686125-4" → "TGBU6861254").
- Packages: Extract the number of packages (PK value).
- Gross Weight: Extract the "Bruto kg" value as a number. If the value uses a period as decimal separator (e.g., "7.453"), convert it to a whole number by removing the decimal point (→ 7453). If the value uses a comma as a decimal separator (e.g., "7,453"), interpret it as a floating-point number (→ 7.453).
- Net Weight: Extract the "Netto kg" value as a number. If the value uses a period as decimal separator (e.g., "6.292"), convert it to a whole number by removing the decimal point (→ 6292). If the value uses a comma as a decimal separator (e.g., "6,292"), interpret it as a floating-point number (→ 6.292).

Return the extracted data in the following JSON format:
{{
    "kaai": "",
    "agent": "",
    "lloydsnummer": "",
    "verblijfsnummer": "",
    "bl": "",
    "artikel_nummer": "",
    "item": "",
    "cost_centers": [
        {{
            "kp": "",
            "invoice_number": "",
            "contract_number": "",
            "company": "",
            "description": "",
            "items": [
                {{
                    "container": "",
                    "packages": 0,
                    "gross_weight": 0,
                    "net_weight": 0
                }}
            ]
        }}
    ]
}}"""

        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Extract specific information from customs documents and return as JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=2000
        )
        
        result = response.choices[0].message.content
        
        # Extract JSON content between triple backticks if present
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', result)
        if json_match:
            result = json_match.group(1)
        
        # Parse the JSON content
        page_data = json.loads(result)
        
        logging.error(json.dumps(page_data, indent=4))
        
        return {
            "page_number": page_num + 1,
            "extracted_data": page_data
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error on page {page_num + 1}: {str(e)}")
        logger.error(f"Raw content: {result}")
        return {
            "page_number": page_num + 1, 
            "error": f"JSON parsing error: {str(e)}",
            "raw_content": result
        }
    except Exception as e:
        logger.error(f"Error processing page {page_num + 1}: {str(e)}")
        return {
            "page_number": page_num + 1,
            "error": str(e)
        }

async def process_pdf_with_openai(pdf_content, file_type):
    """Process PDF content with OpenAI API page by page with controlled concurrency"""
    async with get_openai_client() as client:
        async with open_pdf(pdf_content) as doc:
            total_pages = len(doc)
            
            # Extract text content from all pages first
            pages_text = []
            for page_num in range(total_pages):
                page = doc[page_num]
                text_content = page.get_text()
                pages_text.append(text_content)
        
        # Process pages with controlled concurrency
        pages_data = []
        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        
        async def process_with_semaphore(page_num, text_content):
            async with semaphore:
                if file_type == "inklaringsdocument":
                    return await retry_async(process_page_with_openai_a, client, text_content, page_num, total_pages)
                elif file_type == "afschrijfgegevens":
                    return await retry_async(process_page_with_openai_b, client, text_content, page_num, total_pages)
        
        # Create tasks for each page with controlled concurrency
        tasks = [process_with_semaphore(page_num, text_content) 
                for page_num, text_content in enumerate(pages_text)]
        
        # Process pages and collect results
        pages_data = await asyncio.gather(*tasks)
        
        # Check for errors
        errors = [page for page in pages_data if "error" in page]
        if errors:
            logger.warning(f"Encountered {len(errors)} pages with errors")
            for error in errors:
                logger.warning(f"Page {error['page_number']} error: {error.get('error')}")
        
        # Return combined results
        return {
            "total_pages": total_pages,
            "pages": pages_data
        }

async def process_file(base64_file):
    """Process a single file asynchronously with improved error handling"""
    filename = base64_file.get('filename')
    file_data = base64_file.get('file')

    if not filename or not file_data:
        logger.warning(f"Missing filename or file data")
        return None

    # Check if file is PDF and has correct name pattern
    if not is_pdf(filename):
        logger.info(f"Skipping non-PDF file: {filename}")
        return None

    file_type = check_filename_pattern(filename)
    if not file_type:
        logger.info(f"Skipping file with invalid name pattern: {filename}")
        return None

    try:
        # Decode the base64-encoded file
        decoded_data = base64.b64decode(file_data)

        # Process file with OpenAI based on type
        start_time = time.time()
        extracted_data = await process_pdf_with_openai(decoded_data, file_type)
        elapsed_time = time.time() - start_time
        
        logger.info(f"Processed {filename} in {elapsed_time:.2f} seconds")
        
        return {
            "filename": filename,
            "type": file_type,
            "data": extracted_data
        }

    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}")
        return {
            "filename": filename,
            "type": "error",
            "error": str(e)
        }

async def main_async(req: func.HttpRequest) -> func.HttpResponse:
    """Main asynchronous handler with improved error handling"""
    logger.info('Processing file upload request asynchronously.')
    start_time = time.time()

    try:
        # Parse request body
        try:
            body = req.get_json()
            base64_files = body.get('files', [])
        except Exception as e:
            logger.error(f"Invalid request format: {str(e)}")
            return func.HttpResponse(
                body=json.dumps({"error": f"Invalid request format: {str(e)}"}),
                status_code=400,
                mimetype="application/json"
            )

        if not base64_files:
            logger.warning("No files provided in request")
            return func.HttpResponse(
                body=json.dumps({"error": "No files provided"}),
                status_code=400,
                mimetype="application/json"
            )

        # Process files sequentially to avoid resource issues
        processed_results = []
        for base64_file in base64_files:
            result = await process_file(base64_file)
            if result:
                processed_results.append(result)
                
        # Check if we have required files
        if not processed_results:
            logger.warning("No valid files were processed")
            return func.HttpResponse(
                body=json.dumps({"error": "No valid files were processed"}),
                status_code=400,
                mimetype="application/json"
            )

        # Transform extracted data
        afschrijfgegevens_data = {}
        inklaringsdocument_data = {}

        for result in processed_results:
            if result.get("type") == "afschrijfgegevens":
                afschrijfgegevens_data = transform_afschrijfgegevens(result)
            elif result.get("type") == "inklaringsdocument":
                inklaringsdocument_data = transform_inklaringsdocument(result)

        # Process and merge data
        afschrijfgegevens_data = split_cost_centers(afschrijfgegevens_data)
        logging.error(json.dumps(afschrijfgegevens_data, indent=2))
        result = merge_into_items(inklaringsdocument_data, afschrijfgegevens_data)
        logging.error(json.dumps(result, indent=2))
        
        # Calculate totals (guard against None values returned by the model)
        def _num(value):
            try:
                return float(value or 0)
            except (ValueError, TypeError):
                return 0.0

        result["Total packages"] = sum(_num(item.get("packages")) for item in result.get("Items", []))
        result["Total gross"] = sum(_num(item.get("gross_weight")) for item in result.get("Items", []))
        result["Total net"] = sum(_num(item.get("net_weight")) for item in result.get("Items", []))
        result["Total Value"] = sum(_num(item.get("invoice_value")) for item in result.get("Items", []))
        
        try:
            # Get the ILS number
            response = call_logic_app("UMICORE")

            if response["success"]:
                result["ILS_NUMBER"] = response["doss_nr"]
                logger.info(f"ILS_NUMBER: {result['ILS_NUMBER']}")
            else:
                logger.error(f"❌ Failed to get ILS_NUMBER: {response['error']}")
    
        except Exception as e:
            logger.exception(f"💥 Unexpected error while fetching ILS_NUMBER: {str(e)}")

        # Generate Excel file
        try:
            excel_file = write_to_excel(result)
            logger.info("Generated Excel file.")
            
            reference = result.get("commercial_reference", "")
            if not reference:
                ts = datetime.now().strftime("%y%m%d%H%M%S")
                uid = uuid.uuid4().hex[:4]
                reference = f"UMICORE_{ts}_{uid}"

            # Set response headers for file download
            headers = {
                'Content-Disposition': f'attachment; filename="{reference}.xlsx"',
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            }

            elapsed_time = time.time() - start_time
            logger.info(f"Total processing time: {elapsed_time:.2f} seconds")
            
            # Return Excel file as response
            return func.HttpResponse(
                excel_file.getvalue(), 
                headers=headers, 
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        
        except Exception as e:
            logger.error(f"Error generating Excel: {str(e)}")
            return func.HttpResponse(
                body=json.dumps({"error": f"Error generating Excel: {str(e)}"}),
                status_code=500,
                mimetype="application/json"
            )
    
    except Exception as e:
        logger.error(f"Unhandled exception in main_async: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": f"Error processing request: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )

# Azure Functions entry point
async def main(req: func.HttpRequest) -> func.HttpResponse:
    """Main entry point with timeout protection"""
    try:
        # Create a task for the main processing logic
        task = asyncio.create_task(main_async(req))
        
        # Wait for the task to complete with a timeout
        # Azure Functions have a default timeout of 5 minutes (300 seconds)
        # We'll use a slightly shorter timeout to ensure clean shutdown
        return await asyncio.wait_for(task, timeout=290)
    except asyncio.TimeoutError:
        logger.error("Request processing timed out")
        return func.HttpResponse(
            body=json.dumps({"error": "Request processing timed out"}),
            status_code=408,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error(f"Unhandled exception in main: {str(e)}")
        return func.HttpResponse(
            body=json.dumps({"error": f"Internal server error: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )