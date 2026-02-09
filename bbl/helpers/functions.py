import logging
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

from bbl.helpers.searchOnPorts import search_ports

def process_container_data(data):
    data = data[0]

    # Validate the container format
    container = data.get("container", "")
    valid_container = extract_valid_container(container)

    if not valid_container:
        return # Skip entries without valid containers

    # Process Incoterm
    incoterm = data.get("Incoterm", "")
    incoterm_array = incoterm.split()  # Split into an array of strings

    # Process Freight
    freight = extract_freight(data.get("Freight", "0 USD"))

    # Process Vat 1 and Vat 2
    vat1 = extract_numeric_value(data.get("Vat 1", "0 USD"))
    vat2 = sum(extract_numeric_value(vat) for vat in data.get("Vat 2", "0 EUR").split("+"))

    # Initialize totals
    total_gross_weight = 0.0
    total_net_weight = 0.0
    total_packages = 0.0
    total_devises = 0.0

    # Process items and calculate totals
    items = data.get("items", [])
    for item in items:
        total_gross_weight += item.get("Gross Weight", "0")
        total_net_weight += item.get("Net Weight", "0")
        total_packages += extract_numeric_value(item.get("Packages", "0"))
        total_devises += item.get("VALEUR", "0")  # Assuming VALEUR is in devises

    final_freight, final_vat = calculationVATndFREIGHT(total_devises, freight, vat1, vat2)

    # If incoterm is not provided, set it to an empty array
    if len(incoterm_array) > 1:
        dispatch_country = search_ports(incoterm_array[1])
    else:
        dispatch_country = ""    

    # Reconstruct the processed entry
    processed_entry = {
        "container": valid_container,
        "dispatch_country": dispatch_country,
        "Incoterm": incoterm_array,
        "Freight": round(final_freight, 2),
        "Vat": round(final_vat, 2),
        "items": items,
        "totals": {
            "Gross Weight": total_gross_weight,
            "Net Weight": total_net_weight,
            "Packages": total_packages,
            "DEVISES": total_devises
        }
    }

    return processed_entry

def extract_valid_container(container_string):
    container_arr = container_string.split(" ")
    container = None
    for str in container_arr:
        # Check if the container matches the format 4 chars and 7 digits
        pattern = r'^[A-Z]{4}\d{7}$'
        if re.match(pattern, str):
            container =  str

    return container

def extract_numeric_value(value):
    # Extract numeric value from string and convert to float
    match = re.search(r'[\d,.]+', value)
    if match:
        return float(match.group(0).replace(',', '.'))  # Replace comma with dot for float conversion
    return 0.0

def extract_freight(value):
    # Extract all numeric values from the string
    matches = re.findall(r'[\d,.]+', value)
    
    # Convert the found numbers to floats and replace commas with dots
    numbers = [float(match.replace(',', '.')) for match in matches]

    # Return the list of numbers, limit to two if needed
    return numbers[:2] if numbers else [0.0]

def fetch_exchange_rate(currency_code):
    # Get the current year and month in "YYYYMM" format
    current_date = datetime.now().strftime("%Y%m")

    # Insert the dynamic part into the URL
    url = f"https://www.belastingdienst.nl/data/douane_wisselkoersen/wks.douane.wisselkoersen.dd{current_date}.xml"
    
    # Fetch XML content from the URL
    response = requests.get(url)
    logging.error(url)
    
    if response.status_code == 200:
        # Parse XML content
        root = ET.fromstring(response.content)
        
        # Find the currency block that matches the currency code
        for rate in root.findall("douaneMaandwisselkoers"):
            code = rate.find("muntCode").text
            
            if code == currency_code:
                foreign_rate = rate.find("tariefInVreemdeValuta").text
                return foreign_rate
    
    return None  # Return None if the currency was not found or request failed

def calculationVATndFREIGHT(price, freightUSD, vat1, vat2):
    # Example usage
    currency = 'USD'  # Replace with the desired currency code
    EXCHANGE_RATE = 1.1739#safe_float_conversion(fetch_exchange_rate(currency))

    # First value in freightUSD array is in USD, convert to EUR
    freight_in_usd = freightUSD[0] if len(freightUSD) > 0 else 0
    freight_in_eur = freightUSD[1] if len(freightUSD) > 1 else 0

    # Calculate insurance and freight in EUR
    insurance = price * 0.3 / 100
    freightEUR = (freight_in_usd / EXCHANGE_RATE) + insurance

    # Add the EUR freight value (second value) to the final EUR freight
    total_freightEUR = freightEUR + freight_in_eur

    # Calculate VAT in EUR
    vatEUR = vat1 / EXCHANGE_RATE + vat2

    return [total_freightEUR, vatEUR]

# Helper function to safely convert values to float
def safe_float_conversion(value):
    if value is None:
        return 0.0  # Default to 0 if the value is None
    if isinstance(value, (int, float)):  # If it's already a number, return as is
        return float(value)
    try:
        return float(value.replace(",", "."))  # Try to replace and convert
    except (ValueError, AttributeError):
        print(f"Error converting value: {value}")
        return 0.0  # Handle conversion error, default to 0 or other logic