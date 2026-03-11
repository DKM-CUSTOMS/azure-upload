import logging
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import tempfile
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        "Freight": final_freight,
        "Vat": final_vat,
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
    current_date = datetime.now().strftime("%Y%m")
    url = "https://eservices.minfin.fgov.be/extTariffBrowser/FileResourceForHomePageServlet?fname=listed_currencies.xlsx&lang=EN"
    
    # Cache the file per month locally to avoid fetching it every time
    temp_dir = tempfile.gettempdir()
    cache_file = os.path.join(temp_dir, f"listed_currencies_{current_date}.xlsx")
    
    if not os.path.exists(cache_file):
        try:
            # Fetch the Excel file, ignoring SSL verification as the site's cert might have issues
            response = requests.get(url, verify=False, timeout=15)
            response.raise_for_status()
            with open(cache_file, "wb") as f:
                f.write(response.content)
        except Exception as e:
            logging.error(f"Failed to fetch exchange rate Excel: {e}")
            return None
            
    try:
        df = pd.read_excel(cache_file)
        
        # Month mapping internally to columns based on current month
        month_abbr = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        current_month_idx = datetime.now().month - 1
        current_month_col = month_abbr[current_month_idx]
        
        # Searching the row dynamically for the currency column
        currency_col = None
        for col in df.columns:
            if "Box 22" in str(col):
                currency_col = col
                break
                
        # Fallback to indexing if exact name changes
        if not currency_col:
            currency_col = df.columns[2]
            
        # Match the requested currency code
        match = df[df[currency_col].astype(str).str.strip() == currency_code]
        
        if not match.empty:
            rate = match.iloc[0].get(current_month_col)
            if pd.isna(rate):
                return None
            return str(rate)
            
    except Exception as e:
        logging.error(f"Error parsing exchange rate Excel: {e}")
        return None
        
    return None  # Return None if the currency was not found or request failed

def calculationVATndFREIGHT(price, freightUSD, vat1, vat2):
    # Example usage
    currency = 'USD'  # Replace with the desired currency code
    EXCHANGE_RATE = safe_float_conversion(fetch_exchange_rate(currency))

    # First value in freightUSD array is in USD, convert to EUR
    freight_in_usd = freightUSD[0] if len(freightUSD) > 0 else 0
    freight_in_eur = freightUSD[1] if len(freightUSD) > 1 else 0

    # Handle freight currency logic
    if not freight_in_eur or freight_in_eur == 0:
        final_freight = {"freight": round(freight_in_usd, 2), "currency": "USD"}
    else:
        # Calculate freight in EUR
        freightEUR = (freight_in_usd / EXCHANGE_RATE) if EXCHANGE_RATE else 0
        total_freightEUR = freightEUR + freight_in_eur
        final_freight = {"freight": round(total_freightEUR, 2), "currency": "EUR"}

    # Handle VAT currency logic
    if not vat2 or vat2 == 0:
        final_vat = {"vat": round(vat1, 2), "currency": "USD"}
    else:
        # Calculate VAT in EUR
        vatEUR = (vat1 / EXCHANGE_RATE) + vat2 if EXCHANGE_RATE else 0
        final_vat = {"vat": round(vatEUR, 2), "currency": "EUR"}

    return [final_freight, final_vat]

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