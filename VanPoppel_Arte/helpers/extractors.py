import json
import re
import logging

from AI_agents.OpenAI.custom_call import CustomCall
from global_db.functions.numbers.number_format import parse_number
from VanPoppel_Arte.helpers.validation import INCOTERMS

_CURRENCIES = {
    'USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SEK', 'NOK',
    'DKK', 'PLN', 'CZK', 'HUF', 'RUB', 'BRL', 'MXN', 'INR', 'KRW', 'SGD',
    'HKD', 'NZD', 'ZAR', 'TRY', 'ILS', 'AED', 'SAR', 'THB', 'MYR', 'IDR',
    'PHP', 'VND', 'EGP', 'MAD', 'TND', 'DZD'
}


def collect_numeric_strings(raw_items, footer_text=""):
    """Gather the raw numeric strings of a document for number-format detection."""
    strings = []
    for item in raw_items or []:
        for key in ("net_weight", "surface", "quantity", "unit_price", "amount"):
            value = item.get(key)
            if value:
                m = re.search(r"[\d.,]+", str(value))
                if m:
                    strings.append(m.group(0))
    for pattern in (r"(?:Total incl\.?VAT|Montant TTC|Total inkl. MwSt.):\s+([\d,.]+)",
                    r"Transport\s+([\d,.]+)"):
        m = re.search(pattern, footer_text or "")
        if m:
            strings.append(m.group(1))
    return strings

"""def extract_products_from_text(text):
    lines = text.strip().splitlines()
    products = []
    current_block = []

    for line in lines:
        line = line.strip()
        if re.match(r"^\d{7}$", line):  # product code
            if current_block:
                products.append(current_block)
            current_block = [line]
        elif current_block:
            current_block.append(line)

    if current_block:
        products.append(current_block)
    

    results = []
    for block in products:
        try:
            # 🧼 CLEAN unexpected lines (like 'CONSOL')
            expected_prefixes = [
                "• (", "• Order number:", "• Your reference:", "• Customs Tariff:",
                "• Net:", "• Surface:", "• Country of origin:"
            ]
            cleaned_block = [block[0]]  # keep product code

            for line in block[1:]:
                if any(line.startswith(prefix) for prefix in expected_prefixes):
                    cleaned_block.append(line)
                elif re.match(r"^[\d.,]+\s+[A-Z]{1,3}$", line):  # quantity/unit
                    cleaned_block.append(line)
                elif re.match(r"^[\d.,]+\s*EUR$", line):  # full amount
                    cleaned_block.append(line)
                elif re.match(r"^[\d.,]+$", line):  # only the amount (split line)
                    cleaned_block.append(line)
                elif line.strip() == "EUR" or line.strip() == "USD":  # only currency
                    cleaned_block.append(line)


            # 🩹 Fix broken 'Your reference'
            fixed_block = []
            i = 0
            while i < len(cleaned_block):
                line = cleaned_block[i]
                if line.startswith("• Your reference:") and i + 1 < len(cleaned_block) and not cleaned_block[i + 1].startswith("•"):
                    line += " " + cleaned_block[i + 1]
                    i += 1
                fixed_block.append(line)
                i += 1

            block = fixed_block

            # Validate length
            if len(block) < 11:
                raise ValueError("Block too short after cleaning")

            product_code = block[0]
            product_name = re.search(r"\((.*?)\)\s*(.*)", block[1]).group(2).strip()
            order_number = block[2].split(":", 1)[1].strip()
            reference = block[3].split(":", 1)[1].strip()
            customs_tariff = block[4].split(":", 1)[1].strip()
            net_weight = block[5].split(":", 1)[1].strip()
            surface = block[6].split(":", 1)[1].strip()
            origin = block[7].split(":", 1)[1].strip()

            quantity, unit = block[8].split(" ")
            unit_price = block[9].strip()
            
            # Handle amount split over two lines (e.g., "2089.34", "EUR")
            amount = None
            if len(block) > 10:
                if re.match(r"^[\d,.]+$", block[10]) and len(block) > 11 and ("EUR" in block[11] or "USD" in block[11]):
                    amount = block[10].replace(",", "").strip()
                elif "EUR" in block[10] or "USD" in block[10]:
                    amount = block[10].replace(",", "").replace("EUR", "").replace("USD", "").strip()

            results.append({
                "product_code": product_code,
                "product_name": product_name,
                "order_number": order_number,
                "reference": reference,
                "customs_tariff": customs_tariff,
                "net_weight": net_weight,
                "surface": surface,
                "origin": origin,
                "quantity": quantity,
                "unit": unit,
                "unit_price": unit_price,
                "amount": amount
            })

        except Exception as e:
            logging.error(f"Error processing block: {block}. Error: {e}")
            continue

    return results
"""
def extract_invoice_meta_and_shipping(text):
    meta = {}

    # Account Number / Numéro Client
    acc_match = re.search(r"(Account Number|Numéro Client|Kunden-Nummer)\s+(\d+)", text)
    if acc_match:
        meta["account_number"] = acc_match.group(2)

    # Document Number
    doc_match = re.search(r"(Document Number|Numéro Document|Dokumentnummer)\s+(\d+)", text)
    if doc_match:
        meta["document_number"] = doc_match.group(2)

    # Date
    date_match = re.search(r"(Date|Datum)\s+(\d{2}/\d{2}/\d{4})", text)
    if date_match:
        meta["date"] = date_match.group(2)

    lines = text.splitlines()

    # Billing Address
    billing_address = ""
    billing_start_idx = None

    for i, line in enumerate(lines):
        if "Billing address:" in line or "Adresse de facturation:" in line:
            billing_start_idx = i + 1
            break

    if billing_start_idx is not None:
        for line in lines[billing_start_idx:]:
            if "Account Number" in line or "Numéro Client" in line:  # Stop when Account Number is found
                break
            billing_address += line.strip() + "\n"

    meta["billing_address"] = billing_address.strip()

    # Shipping Address
    shipping_address = ""
    shipping_start_idx = None

    for i, line in enumerate(lines):
        if "Shipping Address" in line:
            shipping_start_idx = i + 1
            break

    if shipping_start_idx is not None:
        for line in lines[shipping_start_idx:]:
            if re.match(r"^\d{7}$", line.strip()):  # product code starts
                break
            shipping_address += line.strip() + "\n"

    meta["shipping_address"] = shipping_address.strip()
    return meta

def extract_totals_and_incoterm(text, fmt="US"):
    """Extract incoterm and totals. Pass the FULL document text — Arte spreads
    the footer over several pages (Incoterms on one page, totals on another)."""
    data = {}
    lines = [line.strip() for line in text.splitlines()]

    # The incoterm value can sit before, on, or after the 'Incoterms:' label
    # depending on the PDF's text-layer ordering. Collect the candidates and
    # keep the first one whose first word is a real Incoterm term.
    term = ""
    location = ""
    for i, line in enumerate(lines):
        if "Incoterms:" in line:
            candidates = [line.split("Incoterms:", 1)[1]]
            if i > 0:
                candidates.append(lines[i - 1])
            if i + 1 < len(lines):
                candidates.append(lines[i + 1])
            for candidate in candidates:
                candidate = (candidate or "").strip()
                if candidate and candidate.split(" ", 1)[0].upper() in INCOTERMS:
                    term = candidate
                    break
            if term:
                break
    for i, line in enumerate(lines):
        if "Location 1:" in line:
            candidates = [line.split("Location 1:", 1)[1]]
            if i > 0:
                candidates.append(lines[i - 1])
            for candidate in candidates:
                candidate = (candidate or "").strip()
                if candidate and not candidate.endswith(":") \
                        and candidate.split(" ", 1)[0].upper() not in INCOTERMS:
                    location = candidate
                    break
            if location:
                break

    if term:
        # location is optional — the term alone is still a valid incoterm
        data["incoterm"] = f"{term} {location}".strip() if location not in term else term

    # US commercial invoices carry a second incoterm for customs ("This
    # commercial invoice is for customs purposes only" followed by e.g.
    # "FCA" / "ZONHOVEN") — that export-leg term is the one the declaration
    # needs, so it overrides the commercial one.
    customs_idx = next((i for i, line in enumerate(lines) if "customs purposes" in line.lower()), None)
    if customs_idx is not None:
        for j in range(customs_idx + 1, min(customs_idx + 4, len(lines))):
            if lines[j].upper() in INCOTERMS:
                place = ""
                if j + 1 < len(lines) and lines[j + 1] and not lines[j + 1].endswith(":"):
                    place = lines[j + 1]
                data["incoterm"] = f"{lines[j]} {place}".strip()
                break

    # Updated regex to handle both "Total incl VAT:" and "Montant TTC:"
    total_match = re.search(r"(?:Total incl\.?VAT|Montant TTC|Total inkl. MwSt.):\s+([\d,.]+)\s+([A-Z]{3})", text)
    if total_match:
        total_str, currency = total_match.groups()
        data["total"] = parse_number(total_str, fmt) or 0.0
        data["currency"] = currency

    # Extract Transport value
    transport_pattern = r"Transport\s+([\d,.]+)"
    transport_match = re.search(transport_pattern, text)
    if transport_match:
        transport_str = transport_match.group(1)
        transport_value = parse_number(transport_str, fmt) or 0.0

        # If transport value is 0.00, check the next line for actual value
        if transport_value == 0.00:
            # Find the position after the matched transport line
            match_end = transport_match.end()
            remaining_text = text[match_end:]

            # Look for the next number (potentially with currency)
            next_value_match = re.search(r"\s+([\d,.]+)(?:\s+[A-Z]{3})?", remaining_text)
            if next_value_match:
                next_value = parse_number(next_value_match.group(1), fmt)
                # Only use this value if it's a valid number and not 0
                if next_value:
                    data["transport"] = next_value
                else:
                    data["transport"] = transport_value
            else:
                data["transport"] = transport_value
        else:
            data["transport"] = transport_value

    return data

def extract_customs_authorization_no(text):
    match = re.search(r"customs \s*([A-Z0-9]+)", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def find_page_in_invoice(doc, keywords=[ "Incoterms:"]):
    try:
        # Open the PDF file
        pdf_document = doc
        
        # Ensure the PDF has at least 1 page
        if len(pdf_document) < 1:
            return "The PDF is empty or has no pages."

        # Search for pages containing all the keywords
        pages_with_data = []
        for page_number in range(len(pdf_document)):
            page = pdf_document[page_number]
            page_text = page.get_text("text")

            # Check if all keywords are found on this page
            if all(keyword in page_text for keyword in keywords):
                pages_with_data.append(page_number + 1)  # Page numbers are 1-based
            
        if pages_with_data:
            return pages_with_data
        else:
            return "No relevant data found in this document."

    except Exception as e:
        return f"An error occurred: {str(e)}"

def extract_customs_code(text_content):
    """Extract customs authorization code from text"""
    custom_call = CustomCall()
    
    # System role for precise extraction
    system_role = """You are a precise data extractor. Extract ONLY the customs authorization code from the text. 
    Return ONLY the code value - no explanations, no labels, no additional text. 
    If no customs authorization code is found, return 'NOT_FOUND'."""
    
    # User prompt
    user_prompt = f"Extract the customs authorization code from this text: {text_content}"
    
    # Send request
    result = custom_call.send_request(system_role, user_prompt)
    
    if result:
        return result.strip()
    else:
        return "EXTRACTION_FAILED"
    
    


def extract_products_from_text(text):
    """Parse product blocks from a page. Returns (results, dropped_product_codes)
    so that silently missing items can be reported instead of vanishing."""
    lines = text.strip().splitlines()
    products = []
    current_block = []

    for line in lines:
        line = line.strip()
        if re.match(r"^\d{7}$", line):  # product code
            if current_block:
                products.append(current_block)
            current_block = [line]
        elif current_block:
            current_block.append(line)

    if current_block:
        products.append(current_block)

    results = []
    dropped = []
    for block in products:
        # not a product block (account numbers etc. also match ^\d{7}$) — skip silently
        if not any("Customs Tariff" in line for line in block):
            continue
        try:
            # 🧼 CLEAN unexpected lines
            expected_prefixes = [
                "• (", "• Order number:", "• Your reference:", "• Customs Tariff:",
                "• Net:", "• Surface:", "• Country of origin:", "• Gross:"
            ]
            cleaned_block = [block[0]]  # keep product code

            for line in block[1:]:
                if any(line.startswith(prefix) for prefix in expected_prefixes):
                    cleaned_block.append(line)
                elif re.match(r"^[\d.,]+\s+[A-Za-z]{1,3}$", line):  # quantity/unit — units can be lowercase (e.g. 'yd')
                    cleaned_block.append(line)
                elif is_currency_amount(line):
                    cleaned_block.append(line)
                elif re.match(r"^[\d.,]+$", line):  # just numbers (including variant codes like 4112025)
                    cleaned_block.append(line)
                elif is_currency_only(line):
                    cleaned_block.append(line)
                else:
                    # Keep product name lines (may wrap over two lines)
                    if len(cleaned_block) <= 2 and not cleaned_block[-1].startswith("• Order"):
                        cleaned_block.append(line)

            # 🩹 FIX: merge stray product name lines if second line not starting with expected label
            if len(cleaned_block) > 2 and not cleaned_block[2].startswith("• Order number:"):
                cleaned_block[1] = cleaned_block[1].strip() + " " + cleaned_block[2].strip()
                del cleaned_block[2]

            # 🩹 Fix broken 'Your reference'
            fixed_block = []
            i = 0
            while i < len(cleaned_block):
                line = cleaned_block[i]
                if line.startswith("• Your reference:") and i + 1 < len(cleaned_block) and not cleaned_block[i + 1].startswith("•"):
                    line += " " + cleaned_block[i + 1]
                    i += 1
                fixed_block.append(line)
                i += 1

            block = fixed_block

            # 🆕 TRY ORIGINAL FORMAT FIRST
            try:
                # Validate length for original format
                if len(block) < 11:
                    raise ValueError("Block too short for original format")

                product_code = block[0]
                product_name_match = re.search(r"\((.*?)\)\s*(.*)", block[1])
                if product_name_match:
                    product_name = product_name_match.group(2).strip()
                else:
                    product_name = block[1].strip()

                order_number = block[2].split(":", 1)[1].strip()
                reference = block[3].split(":", 1)[1].strip()
                customs_tariff = block[4].split(":", 1)[1].strip()
                net_weight = block[5].split(":", 1)[1].strip()
                surface = block[6].split(":", 1)[1].strip()
                origin = block[7].split(":", 1)[1].strip()

                quantity, unit = block[8].split(" ")
                unit_price = block[9].strip()
                amount, currency = extract_amount_and_currency(block[10:12])
                
            except (ValueError, IndexError, AttributeError):
                # 🆕 FALLBACK TO NEW FORMAT
                logging.info(f"Trying new format for block starting with {block[0]}")
                
                product_code = block[0]
                product_name_match = re.search(r"\((.*?)\)\s*(.*)", block[1])
                if product_name_match:
                    product_name = product_name_match.group(2).strip()
                else:
                    product_name = block[1].strip()

                order_number = block[2].split(":", 1)[1].strip()
                reference = block[3].split(":", 1)[1].strip()
                
                # Find the line with Customs Tariff (might be combined with Gross)
                customs_line_idx = next((i for i, line in enumerate(block) if "Customs Tariff:" in line), None)
                if not customs_line_idx:
                    raise ValueError("Cannot find Customs Tariff line")
                
                # Parse Customs Tariff (might have Gross on same line)
                customs_line = block[customs_line_idx]
                customs_match = re.search(r"Customs Tariff:\s*(\S+)", customs_line)
                customs_tariff = customs_match.group(1) if customs_match else ""
                
                # 🔧 FIX: Look for Net line more flexibly (check for "Net:" anywhere in the line)
                net_line_idx = next((i for i, line in enumerate(block) if "Net:" in line), None)
                if net_line_idx is not None:
                    net_match = re.search(r"Net:\s*([\d.,]+\s*[A-Z]+)", block[net_line_idx])
                    net_weight = net_match.group(1).strip() if net_match else ""
                else:
                    net_weight = ""
                
                # Find Surface and Origin
                surface_idx = next((i for i, line in enumerate(block) if "Surface:" in line), None)
                origin_idx = next((i for i, line in enumerate(block) if "Country of origin:" in line), None)
                
                if not surface_idx or not origin_idx:
                    raise ValueError("Cannot find Surface or Origin")
                
                surface = block[surface_idx].split(":", 1)[1].strip()
                origin = block[origin_idx].split(":", 1)[1].strip()
                
                # Find the quantity line: number + unit where the unit is NOT a
                # currency (units may be lowercase, e.g. '31.00 yd'); searching by
                # shape instead of position survives extra/missing lines
                qty_idx = None
                for j in range(origin_idx + 1, len(block)):
                    m = re.match(r"^([\d.,]+)\s+([A-Za-z]{1,3})\s*$", block[j])
                    if m and m.group(2).upper() not in _CURRENCIES:
                        qty_idx = j
                        quantity, unit = m.group(1), m.group(2)
                        break
                if qty_idx is None:
                    raise ValueError("Cannot find quantity/unit in new format")

                # After the quantity: unit price, optional discount %, amount, currency.
                # e.g. ['42.11 USD', '1,305.33', 'USD']            (US layout)
                #      ['336.00 USD', '40', '1,411.20', 'USD']     (with 40% discount)
                #      ['70.88 EUR', '141.75 EUR']                 (EU layout)
                values = []   # (number_string or None, currency or None)
                for j in range(qty_idx + 1, len(block)):
                    line = block[j].strip()
                    m = re.match(r"^([\d.,]+)\s*([A-Za-z]{3,4})?$", line)
                    if m:
                        values.append((m.group(1), (m.group(2) or "").upper() or None))
                    elif is_currency_only(line):
                        values.append((None, line))
                numeric = [v for v in values if v[0] is not None]
                if not numeric:
                    raise ValueError("No price/amount lines found in new format")
                unit_price = numeric[0][0]
                amount = numeric[-1][0] if len(numeric) > 1 else None
                currency = next((c for _, c in reversed(values) if c), None)

            results.append({
                "product_code": product_code,
                "product_name": product_name,
                "order_number": order_number,
                "reference": reference,
                "customs_tariff": customs_tariff,
                "net_weight": net_weight,
                "surface": surface,
                "origin": origin,
                "quantity": quantity,
                "unit": unit,
                "unit_price": unit_price,
                "amount": amount,
                "currency": currency
            })

        except Exception as e:
            logging.error(f"Error processing block: {block}. Error: {e}")
            dropped.append(block[0] if block else "?")
            continue

    return results, dropped


def is_currency_amount(line):
    """
    Check if line contains amount with currency
    Matches patterns like: "123.45 EUR", "1,234.56EUR", "999 USD", etc.
    """
    # Pattern for number followed by currency (3-4 letter currency codes)
    return bool(re.match(r"^[\d.,]+\s*[A-Z]{3,4}$", line.strip()))


def is_currency_only(line):
    """
    Check if line contains only currency code
    Matches common currency codes (3-4 letters, all uppercase)
    """
    line = line.strip()
    # Common currency pattern: 3-4 uppercase letters
    return bool(re.match(r"^[A-Z]{3,4}$", line)) and len(line) <= 4


def extract_amount_and_currency(amount_lines):
    """
    Extract amount and currency from the remaining lines after unit_price
    Handles both single line (amount + currency) and split lines scenarios
    """
    if not amount_lines:
        return None, None
    
    # Try first line - check if it has both amount and currency
    first_line = amount_lines[0].strip()
    
    # Check if first line has amount + currency in one line
    # NOTE: amounts are returned RAW — the document-level number format
    # parser decides later whether commas/dots are thousands or decimals
    currency_match = re.search(r"^([\d.,]+)\s*([A-Z]{3,4})$", first_line)
    if currency_match:
        return currency_match.group(1), currency_match.group(2)

    # Check if it's split across two lines
    if len(amount_lines) >= 2:
        # First line: amount only
        if re.match(r"^[\d.,]+$", first_line):
            second_line = amount_lines[1].strip()
            # Second line: currency only
            if is_currency_only(second_line):
                return first_line, second_line

    # If no clear pattern found, try to extract what we can
    # Look for any currency in the lines
    for line in amount_lines:
        currency_in_line = re.search(r"([A-Z]{3,4})", line)
        if currency_in_line:
            currency = currency_in_line.group(1)
            # Extract numbers from the same or previous lines
            amount_match = re.search(r"([\d.,]+)", line)
            if amount_match:
                return amount_match.group(1), currency

    return None, None


# Alternative approach: Build currency list dynamically
def get_detected_currencies(text):
    """
    Scan the entire text to detect all currencies used
    This can be called once before processing to build a currency list
    """
    currencies = set()
    
    # Find all potential currency codes (3-4 uppercase letters)
    potential_currencies = re.findall(r'\b[A-Z]{3,4}\b', text)
    
    # Filter to keep only likely currencies (you can expand this list)
    known_currencies = {
        'USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'SEK', 'NOK',
        'DKK', 'PLN', 'CZK', 'HUF', 'RUB', 'BRL', 'MXN', 'INR', 'KRW', 'SGD',
        'HKD', 'NZD', 'ZAR', 'TRY', 'ILS', 'AED', 'SAR', 'THB', 'MYR', 'IDR',
        'PHP', 'VND', 'EGP', 'MAD', 'TND', 'DZD'  # Added some African currencies
    }
    
    for curr in potential_currencies:
        if curr in known_currencies:
            currencies.add(curr)
    
    return currencies   