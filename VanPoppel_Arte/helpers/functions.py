import datetime
import logging
from bs4 import BeautifulSoup

from global_db.countries.functions import get_abbreviation_by_country
from global_db.functions.numbers.number_format import parse_number


def merge_invoice_outputs(invoice_outputs):
    if not invoice_outputs:
        return {}

    # Take header and footer base from first invoice
    merged_output = {
        "header": invoice_outputs[0]["header"],
        "customs_no": invoice_outputs[0]["customs_no"],
        "items": [],
        "footer": {
            "incoterm": None,
            "currency": None,
            "total": 0.0,
            "transport": 0.0
        }
    }

    total_sum = 0.0
    transport_sum = 0.0
    currency_set = set()

    for i, invoice in enumerate(invoice_outputs):
        # Merge items
        merged_output["items"].extend(invoice.get("items", []))

        # Get footer info
        footer = invoice.get("footer", {})
        total = footer.get("total", 0)
        transport = footer.get("transport", 0)
        currency = footer.get("currency")

        if i == 0:
            merged_output["footer"]["incoterm"] = footer.get("incoterm")
            merged_output["footer"]["currency"] = currency

        if total:
            try:
                total_sum += float(total)
            except:
                pass
            
        if transport:
            try:
                transport_sum += float(transport)
            except:
                pass

        if currency:
            currency_set.add(currency)

    merged_output["footer"]["total"] = round(total_sum, 2)
    merged_output["footer"]["transport"] = round(transport_sum, 2)

    return merged_output

def safe_float_conversion(value):
    try:
        return float(value)
    except ValueError:
        return 0

def safe_int_conversion(value):
    try:
        return int(value)
    except ValueError:
        return 0
    
def clean_invoice_items(combined_result):
    cleaned_items = []
    TotalNetWeight = 0
    TotalSurface = 0
    TotalQuantity = 0
    
    headerdata = combined_result.get("header", [])
    date = headerdata.get("date", [])
    
    # change the invoice date to date format
    if date:
            formats = ["%d.%m.%Y", "%d/%m/%Y"]  # Supported date formats
            for date_format in formats:
                try:
                    date = datetime.datetime.strptime(date, date_format).date()
                except ValueError:
                    logging.error(f"Invalid date format: {date}")

    for item in combined_result.get("items", []):
        try:
            # number format detected per source document (EU vs US separators)
            fmt = item.pop("_fmt", "US")
            net_weight = parse_number(str(item.get("net_weight") or "").replace("KG", "").strip(), fmt) or 0.0
            surface = parse_number(str(item.get("surface") or "").replace("M2", "").strip(), fmt) or 0.0
            quantity = int(round(parse_number(str(item.get("quantity") or "").strip(), fmt) or 0))
            unit_price = parse_number(str(item.get("unit_price") or "").replace("EUR", "").replace("USD", "").strip(), fmt) or 0.0
            amount = parse_number(str(item.get("amount") or "").replace("EUR", "").replace("USD", "").strip(), fmt) or 0.0

            TotalNetWeight += net_weight
            TotalSurface += surface
            TotalQuantity += quantity

            cleaned_item = {
                "product_code": item.get("product_code", "").strip(),
                "product_name": item.get("product_name", "").strip(),
                "order_number": item.get("order_number", "").strip(),
                "reference": item.get("reference", "").strip(),
                "customs_tariff": item.get("customs_tariff", "").strip(),
                "origin": get_abbreviation_by_country(item.get("origin", "").strip()),

                "net_weight": net_weight,
                "surface": surface,
                "quantity": quantity,
                "unit": item.get("unit", "").strip(),

                "unit_price": unit_price,
                "amount": amount,

                "document_number": item.get("document_number", "").strip(),
                "date": date,
            }

            cleaned_items.append(cleaned_item)

        except Exception as e:
            logging.error(f"Error processing item {item}: {e}")

    # Replace items with cleaned version
    combined_result["items"] = cleaned_items
    return [combined_result, TotalNetWeight, TotalSurface, TotalQuantity]

def extract_email_body(html_content):
    """
    Extracts the visible body text from an Outlook HTML email.
    
    Args:
        html_content (str): The raw HTML content of the email.
    
    Returns:
        str: Cleaned plain-text body of the email.
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Optionally: remove script and style elements
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()

    # Find the <body> tag content if it exists
    body = soup.find("body")
    text = body.get_text(separator="\n") if body else soup.get_text(separator="\n")

    # Clean extra spaces and lines
    clean_text = '\n'.join(line.strip() for line in text.splitlines() if line.strip())

    return clean_text
    