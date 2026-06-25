import json
import logging
import re
from bs4 import BeautifulSoup


def clean_incoterm(inco : str) -> list :
    # Always return exactly [term, place] so callers can unpack safely,
    # even when the AI returns no incoterm or one with no place (e.g. "EXW").
    if inco:
        parts = inco.split(' ', maxsplit=1)
        return [parts[0], parts[1] if len(parts) > 1 else '']
    return ['', '']

def clean_Origin(value : str) -> str :
    return value.replace("Origin:", "").replace("d'origine:", "")

def clean_HS_code(value : str) -> str :
    return value.replace(",", "")

def normalize_numbers(number_str : str) -> float:
    """
    Normalize a number string to a consistent float format.
    :param number_str: A string representing a number (e.g., "3.158,6" or "28,158.23").
    :return: A float representing the normalized number.
    """
    normalized = ""
    # Handle comma as decimal separator
    if re.match(r"^\d{1,3}(\.\d{3})*,\d{1,2}$", number_str):
        # Replace dots (thousands separator) with nothing, replace comma (decimal) with a dot
        normalized = number_str.replace('.', '').replace(',', '.')
    # Handle dot as decimal separator
    elif re.match(r"^\d{1,3}(,\d{3})*\.\d{1,2}$", number_str):
        # Replace commas (thousands separator) with nothing
        normalized = number_str.replace(',', '')
    
    return normalized

def normalize_numbers_gross(number_str: str) -> float:
    """
    Normalize a number string to a consistent float format.
    :param number_str: A string representing a number (e.g., "3.158,6" or "28,158.23").
    :return: A float representing the normalized number.
    """
    # Handle comma as decimal separator
    if re.match(r"^\d{1,3}(\.\d{3})*,\d{1,2}$", number_str):
        # Replace dots (thousands separator) with nothing, replace comma (decimal) with a dot
        normalized = number_str.replace('.', '').replace(',', '.')
    # Handle dot as decimal separator
    elif re.match(r"^\d{1,3}(,\d{3})*\.\d{1,2}$", number_str):
        # Replace commas (thousands separator) with nothing
        normalized = number_str.replace(',', '')
    # Handle cases with only thousands separator (no decimal part)
    elif re.match(r"^\d{1,3}(\.\d{3})*$", number_str):
        # Replace dots (thousands separator) with nothing
        normalized = number_str.replace('.', '')
    elif re.match(r"^\d{1,3}(,\d{3})*$", number_str):
        # Replace commas (thousands separator) with nothing
        normalized = number_str.replace(',', '')
    else:
        # If the format is not recognized, return None
        return None

    try:
        return float(normalized)
    except ValueError:
        return None

def clean_number_from_chars(value: str) -> str:
    # Use regex to keep only digits, commas, and periods
    cleaned = re.sub(r'[^\d.,]', '', value)
    return cleaned

def clean_customs_code(value : str) -> str:
    return value.replace(')', '')

def safe_int_conversion(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0

def safe_float_conversion(value) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.00 
    
def extract_and_clean(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = soup.get_text()
    
    return data    

def extract_container_number(text):
    """
    Extract the container number from a given text.
    The format is: Two characters (letters), optional space, four numbers.
    
    Args:
        text (str): The input text.
    
    Returns:
        str or None: The container number if found, otherwise None.
    """
    pattern = r"\b([A-Za-z]{4}\d{4})\b"
    match = re.search(pattern, text)
    return match.group(1) if match else ""

def extract_Exitoffice(text):
    """
    Extract the container number from a given text.
    The format is: Two characters (letters), optional space, four numbers.
    
    Args:
        text (str): The input text.
    
    Returns:
        str or None: The container number if found, otherwise None.
    """
    pattern = r"\b([A-Za-z]{2}\s?\d{6})\b"
    match = re.search(pattern, text)
    return match.group(1) if match else ""

def merge_json_objects(json_objects):
    # Initialize the output with the first object
    merged_object = json_objects[0].copy()

    # Function to handle value joining only if the values differ
    def join_values_if_diff(key, merged_obj, obj_list):
        values = [obj.get(key) for obj in obj_list if key in obj and obj[key] is not None]
        if values:
            # Only join if the values are different
            unique_values = set(values)
            if len(unique_values) > 1:  # Values are different
                merged_obj[key] = '+'.join(unique_values)
            else:
                merged_obj[key] = values[0]

    # Iterate over the other JSON objects
    for obj in json_objects[1:]:
        # Join values for the specified fields with "+" if different
        join_values_if_diff("Inv Reference", merged_object, [merged_object, obj])
        join_values_if_diff("Other Ref", merged_object, [merged_object, obj])  

        # Sum fields like Total, Freight, and Gross weight Total
        if "Total" in obj and obj["Total"] is not None:
            if "Total" in merged_object and merged_object["Total"] is not None:
                merged_object["Total"] += obj["Total"]
            else:
                merged_object["Total"] = obj["Total"]
                
        # Sum fields like Total, Freight, and Gross weight Total
        if "Total pallets" in obj and obj["Total pallets"] is not None:
            if "Total pallets" in merged_object and merged_object["Total pallets"] is not None:
                merged_object["Total pallets"] += obj["Total pallets"]
            else:
                merged_object["Total pallets"] = obj["Total pallets"]

        if "Freight" in obj and obj["Freight"] is not None:
            if "Freight" in merged_object and merged_object["Freight"] is not None:
                merged_object["Freight"] += obj["Freight"]
            else:
                merged_object["Freight"] = obj["Freight"]

        if "Gross weight Total" in obj and obj["Gross weight Total"] is not None:
            if "Gross weight Total" in merged_object and merged_object["Gross weight Total"] is not None:
                merged_object["Gross weight Total"] += obj["Gross weight Total"]
            else:
                merged_object["Gross weight Total"] = obj["Gross weight Total"]

        if "Total net" in obj and obj["Total net"] is not None:
            if "Total net" in merged_object and merged_object["Total net"] is not None:
                merged_object["Total net"] += obj["Total net"]
            else:
                merged_object["Total net"] = obj["Total net"]

        # Append Items items
        if "Items" in obj and obj["Items"] is not None:
            if "Items" not in merged_object or merged_object["Items"] is None:
                merged_object["Items"] = []
            merged_object["Items"].extend(obj["Items"])

        # Copy values for Address, Incoterm, and Origin (ensure to not overwrite)
        for key in ["Incoterm", "Address"]:
            if key in obj and obj[key] is not None and (key not in merged_object or merged_object[key] is None):
                merged_object[key] = obj[key]

    return merged_object

async def process_page_with_openai_a(client, text_content):
    """Process a single page with OpenAI API for inklaringsdocument"""
    try:
        response = await client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Extract specific information from customs documents and return as JSON."},
                {"role": "user", "content": f"""Extract the following details from this email and return them as a structured JSON object: 
                {text_content}

                You are a document parser specialized in processing logistics/exportation request emails in Dutch.

                Your job is to extract structured data in raw JSON only — no formatting, no styling, no explanation. Focus on precision and consistency.
                
                Your Rules:
                Only respond with valid, raw JSON.
                
                eur1 and atr are booleans.
                
                If the line + EUR1 or similar contains an X or x before it (e.g. x + EUR1 or X + EUR1), mark eur1: true. Else, false.
                
                Same logic for + ATR.
                
                Ignore + sign — it means nothing.
                
                Extract all listed items with the following fields (if present):
                
                no
                
                item_code
                
                product_naam
                
                aantal_stuks
                
                eenheid
                
                net_weight
                
                gross_weight
                
                imaj_document_number
                
                If any field is missing, skip it or leave it blank (e.g. gross_weight: "").
                
                Do not change values. Keep units and formatting (e.g. "2400.00 KG").
                
                Order of items matters — keep the same sequence as in the original document.
                
                Never explain. Output must be only JSON."""}
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
        
        return {
            "extracted_data": page_data
        }
        
    except json.JSONDecodeError as e:
        logging.error(f"Raw content: {result}")
        return {
            "error": f"JSON parsing error: {str(e)}",
            "raw_content": result
        }
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return {
            "error": str(e)
        }
               
def add_statistical_values(data):
    # Safety checks
    freight = data.get("Freight")
    total_net = data.get("Total net")
    items = data.get("Items", [])

    if not isinstance(freight, (int, float)) or freight == 0:
        print("Invalid or zero Freight. Skipping calculation.")
        return data

    if not isinstance(total_net, (int, float)) or total_net == 0:
        print("Invalid or zero Total net. Skipping calculation.")
        return data

    # Loop through items and apply deduction logic
    for item in items:
        net_weight = item.get("Net weight", 0.0)
        if not isinstance(net_weight, (int, float)) or net_weight == 0:
            item["deduction_price"] = 0.0
            item["statistical_value"] = item.get("Price", 0.0)
            continue

        price = item.get("Price", 0.0)
        deduction = round((freight * net_weight) / total_net, 4)
        statistical_value = round(price + deduction, 4)

        item["deduction_price"] = deduction
        item["statistical_value"] = statistical_value

    return data
        