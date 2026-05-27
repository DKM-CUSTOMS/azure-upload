import fitz  # PyMuPDF
import openpyxl
import json
from io import BytesIO
import re
from bs4 import BeautifulSoup

from export.helpers.adresseExtractor import get_adress_structure

def extract_key_value_pairs(text):
    lines = text.strip().splitlines()  # Split the text into individual lines
    key_value_pairs = {}  # Dictionary to store the key-value pairs
    current_key = None
    current_value = []
    extracting = False  # Flag to determine if we are within the extraction range

    for line in lines:
        line = line.strip()
        
        # Start extraction when we hit "Aangifte"
        if line == "Aangifte":
            extracting = True
        
        # Stop extraction after reaching "AEO nummer"
        if line == "AEO nummer":
            current_key = "AEO nummer"
            current_value = []  # Reset value for "AEO nummer"
            continue  # Skip the value assignment for now
        
        # If extraction hasn't started yet, continue
        if not extracting:
            continue

        # Check for separator lines
        if line == "_________________________________________________________________________":
            # If we have a current key, save the key and value
            if current_key:
                key_value_pairs[current_key] = " ".join(current_value).strip() if current_value else ""
            current_key = None
            current_value = []
        elif current_key is None:
            # Treat the first line after a separator as the key
            current_key = line
        else:
            # All subsequent lines are considered part of the value
            current_value.append(line)

    # Handle the last key-value pair
    if current_key:
        key_value_pairs[current_key] = " ".join(current_value).strip() if current_value else ""

    # Remove space after colon in AEO nummer (e.g. "Y022: BE..." -> "Y022:BE...")
    if "AEO nummer" in key_value_pairs:
        key_value_pairs["AEO nummer"] = re.sub(r':\s+', ':', key_value_pairs["AEO nummer"])

    return key_value_pairs

def extract_text_from_pdf(pdf_path):
    # Open the PDF file
    pdf_document = fitz.open(pdf_path)
    
    # Extract text from all pages
    text = ""
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text += page.get_text()

    return text

def insert_after_key(data, insert_key, insert_value, target_key):
    new_data = {}

    for key, value in data.items():
        new_data[key] = value

        # Check if we have reached the target key to insert after
        if key == target_key:
            # Insert the new key-value pair after the target key
            new_data[insert_key] = insert_value

    return new_data

def updateBTWnumber(json_string):
    data = json.loads(json_string)

    # Check if the key exists
    for key, value in data.items():
        if key == "BTW-nummer":
            # Find the split point for the "BTW-nummer"
            parts = value.split(' ', 2)  # Split into at most 3 parts

            # Construct the new value for "BTW-nummer" from the first two parts
            new_value = ' '.join(parts[:2]).strip()  # Combine the first two parts

            # Construct the new value for "Exportformaliteiten te verrichten op een"
            remaining_address = value[len(new_value):].strip()  # Get the remaining address

            # Update the fields
            data["BTW-nummer"] = new_value
            data = insert_after_key(data, "Adress", remaining_address, "BTW-nummer")

            break  # Exit loop once the key is found

    return json.dumps(data)  # Return the updated dictionary directly

def updateAdress(json_string, text):
    data = json.loads(json_string)

    keys = ["COUNTRY", "POSTALCODE",  "City", "Street", "Name"]

    # Check if the key exists
    for key, value in data.items():
        if key == "Adress":
            arr = get_adress_structure(text)
            
            i = 0
            for item  in list(reversed(arr)) :
                data = insert_after_key(data, keys[i], item, "BTW-nummer")
                i += 1
                
            data.pop('Adress')
            break  # Exit loop once the key is found

    return json.dumps(data)  # Return the updated dictionary directly

def write_to_excel(json_string, number):
    # Create a new workbook and select the active sheet
    wb = openpyxl.Workbook()
    ws = wb.active

    data = json.loads(json_string)

    # Prepare row keys (headers) and row values
    row_string_exit_office = ["Exit office", "ILS NUMBER"]
    row_number = [number, data["ILS_NUMBER"]]
    row_keys = []
    row_values = []

    for key, value in data.items():
        if key == "Exportformaliteiten te verrichten op een":
            break

        row_keys.append(key)

        # Handle array values
        if isinstance(value, list):
            if len(value) > 0:
                row_values.append(value[0])
                if len(value) > 1:
                    row_keys.append(key + " valuta")
                    row_values.append(value[1])  # Add second element of array to supplemental row
            else:
                row_values.append("")
        else:
            row_values.append(value)    

    # Add values to the second row
    ws.append(row_string_exit_office)          
    ws.append(row_number)

    # Add keys (headers) to the first row
    ws.append(row_keys)

    # Add values to the second row
    ws.append(row_values)

    # Optionally, adjust column widths for better formatting
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter  # Get the column name
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width

    # Save the workbook to a BytesIO object (in memory)
    file_stream = BytesIO()
    wb.save(file_stream)
    file_stream.seek(0)

    return file_stream

def correct_number_format(value):
    # Remove periods and replace commas with periods
    corrected_value = re.sub(r'\.', '', value)  # Remove periods
    corrected_value = re.sub(r'(?<=\d),(?=\d)', '.', corrected_value)  # Replace comma with period if it’s between numbers
    return corrected_value if corrected_value == "" else float(corrected_value)

def modify_and_correct_amounts(data):
    # Ensure the input is a string and convert to dictionary
    if isinstance(data, str):
        data = json.loads(data)  # Convert JSON string to dictionary
    elif not isinstance(data, dict):
        raise TypeError("Input must be a JSON string or a dictionary.")
    
    # Modify the specified keys by splitting their values
    for key in ["Aanvullende eenheden", "Nettogewicht", "Brutogewicht"]:
        if key in data:
            value = data[key]
            # Split the value into amount and currency
            corrected_amount = correct_number_format(value)  # Correct the number format
            data[key] = corrected_amount  # Update the value to be an array

    # Modify the specified keys by splitting their values
    for key in ["Totaal gefactureerd bedrag", "Statistische waarde"]:
        if key in data:
            value = data[key]
            # Split the value into amount and currency
            amount, currency = value.split()  # Assuming the format is "amount currency"
            corrected_amount = correct_number_format(amount)  # Correct the number format
            data[key] = [corrected_amount, currency]  # Update the value to be an array

    return json.dumps(data, indent=2)

# Regex patterns for 'Exit office' and 'Kantoor'
exit_office_pattern = r"Exit\s*office[:;]?\s*(BE?\s*\d{5,8})"
kantoor_pattern = r"Kantoor[:;]?\s*(B?E?\s*\d{5,8})"

def extract_body_text(html_content):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Extract text from the body tag, if it exists
    if soup.body:
        body_text = soup.body.get_text(separator="\n").strip()
        return body_text
    else:
        return "No body tag found."

# Function to extract the values
def extract_office_value(text):
    # Search for 'Exit office'
    exit_office_match = re.search(exit_office_pattern, text, re.IGNORECASE)
    if exit_office_match:
        resultat = exit_office_match.group(1)
        resultat = resultat if not resultat else resultat.replace(" ", "")
        return resultat
    
    # Search for 'Kantoor'
    kantoor_match = re.search(kantoor_pattern, text, re.IGNORECASE)
    if kantoor_match:
        resultat = kantoor_match.group(1)
        resultat = resultat if not resultat else resultat.replace(" ", "")
        return resultat
    
    return None