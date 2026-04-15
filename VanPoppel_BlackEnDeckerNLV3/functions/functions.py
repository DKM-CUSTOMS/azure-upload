import logging
from bs4 import BeautifulSoup
import json
from AI_agents.Mistral.MistralDocumentQAFiles import MistralDocumentQAFiles
from AI_agents.OpenAI.custom_call import CustomCall

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

def test_extract_items_from_pdf(base64_pdf: str, filename):
    prompt = """
    Extract all invoice items from the provided document(s) and return the results in strict JSON format as specified below.
        JSON Structure Requirements:
        {
          "Items": [
            {
              "InvoiceNumber": "string",
              "InvoiceDate": "dd-mm-yyyy",
              "Description": "string",
              "HSCode": "string",
              "Origin": "string",
              "NetWeight": number,
              "Quantity": number,
              "Amount": number,    // **ALWAYS the LAST numeric value in the row located in the last column (Amount)**
              "Currency": "string"
            }
          ]
        }
        
        Extraction Rules:

        No Omissions:

        Extract every line item from all pages of the invoice.
        Do not stop after a fixed number of rows.
        If the invoice spans multiple pages, combine all items into a single JSON array.

        Data Formatting:

        Dates: Always use dd-mm-yyyy format.
        Numbers:

        Use dots (.) for decimals (e.g., 374.00).
        Remove thousands separators (e.g., 10,000 → 10000).
        Ensure NetWeight, Quantity, UnitPrice, and Amount are numeric.


        Currency: Always include the 3-letter currency code (e.g., GBP, USD).
        Empty Fields: If a field (e.g., Origin) is missing, use an empty string "".


        Column Mapping:

        Description: Use the item description (e.g., MFX Rivet Stainl Steel DH 4.8x30).
        HSCode: Use the commodity code (e.g., 8308200090).
        NetWeight: Extract the individual item weight (e.g., 12.400) always near KG extract the exact one and always the (.) are decimal seprators, also the net weight always has a 3 numbers after decimal dot make sure u extract the number correct.

        If only the total weight is provided, distribute it proportionally by quantity.
        Amount: ALWAYS the LAST numeric value in the row (regardless of column headers).

        Example: If the row ends with 81.74, that is the Amount.

        Validation:

        Do not calculate or derive values.
        Do not cross-check Quantity × UnitPrice = Amount.
        If the document is unclear, flag ambiguous fields with empty string like "" and request clarification.

        Output:

        Return only valid JSON—no explanations, notes, or placeholders.
        If the document is unclear, flag ambiguous fields with empty string like "".

        Example Output:
        {
          "Items": [
            {
              "InvoiceNumber":,
              "InvoiceDate": ,
              "Description": ",
              "HSCode": ",
              "Origin": ,
              "NetWeight": ,
              "Quantity": ,
              "Amount": ,
              "Currency":
            },
            {
              "InvoiceNumber": ,
              "InvoiceDate": ,
              "Description": ,
              "HSCode": ,
              "Origin":,
              "NetWeight":,
              "Quantity": ,
              "Amount": ,
              "Currency": 
            }
          ]
        }

        Key Clarifications:

        Amount is always the last numeric value in the row. don't mix it up with UnitPrice, to distinguish them, always take the last big numeric value in the row as Amount.
        the unit price is not required in the output.
        the unit price is the numeric value with a currency before the Amount.
        the amount is the last numeric value in the row and don't have a currency because the currency is in the column header.
        make sure u don't put the unit price in the amount field.
        u can check the total amount at the bottom of the invoice to make sure u extracted the amount correctly.
        If the invoice has multiple pages, extract items from all pages and combine them into a single JSON array.
    """

    # Mistral call
    qa = MistralDocumentQAFiles()
    response = qa.ask_document(base64_pdf, prompt, filename=filename)

    # Clean response
    raw = response.replace("```", "").replace("json", "").strip()
    parsed = json.loads(raw)
    
    logging.error(f"Extracted JSON: {json.dumps(parsed, indent=2)}")
    
    return parsed

#------------------- Extract items with AI ---------------------------'''
def extract_clean_excel_from_pdf(doc_text: str):

    prompt = f"""
Extract all invoice items from the provided text and return strict JSON as specified below.

--- DOCUMENT LAYOUT ---

The table has 7 columns defined across TWO header rows:

  Col 1          | Col 2            | Col 3  | Col 4       | Col 5    | Col 6 | Col 7
  Material       | Description      |        |             | Quantity | Price | Amount
  Your Reference | Commodity code   | Origin | Net Weight  |          |       | <Currency>

Each invoice ITEM spans exactly TWO consecutive data rows:
  Row A (top):    Material number | Item description | Quantity + unit | Unit Price + currency | Amount
  Row B (bottom): Your Reference  | Commodity code (HSCode) | Origin | Net Weight + KG

Pair Row A and Row B together to build one item. Never treat them as two separate items.

There are also METADATA rows injected between items. They are NOT items. They look like:
  "Order 1000741454 date"
  "Order 1000743622 date 13/04/2026 Your Ref. 151"
  "Delivery 81256305 date 14/04/2026"

From these metadata rows extract:
  - InvoiceNumber: the number immediately after the word "Order"
  - InvoiceDate:   the date immediately after the word "date" (format dd-mm-yyyy)
  Apply the extracted InvoiceNumber and InvoiceDate to all items that follow until the next metadata row.

--- JSON OUTPUT ---

{{
  "Items": [
    {{
      "InvoiceNumber": "string",
      "InvoiceDate": "dd-mm-yyyy",
      "Description": "string",
      "HSCode": "string",
      "Origin": "string",
      "NetWeight": number,
      "Quantity": number,
      "Amount": number,
      "Currency": "string"
    }}
  ]
}}

--- EXTRACTION RULES ---

No Omissions:
- Extract every line item from all pages.
- Combine all pages into a single JSON array.

Field Mapping (using the two-row structure above):
- Description:   Row A, Col 2 — the item description text.
- HSCode:        Row B, Col 2 — the commodity code (e.g. 8467190000).
- Origin:        Row B, Col 3 — country of origin (e.g. CN).
- NetWeight:     Row B, Col 4 — numeric value near "KG" (3 decimal places, e.g. 3.000).
- Quantity:      Row A, Col 5 — numeric value (ignore the unit label e.g. EA).
- Amount:        The VERY LAST numeric value in Row A — it is a standalone number at the end of the row with NO unit, NO currency, and NO label after it.

  Row A example:  23025753 | DPN908-015 | 5 EA | Price | 830.44 EUR | 1000 EA | 4.15
  The price section contains THREE values: the word "Price", then a unit price WITH currency (830.44 EUR), then a pack quantity WITH unit (1000 EA).
  NONE of these are the Amount. The Amount is 4.15 — the final lone number at the end.

  Rule: scan Row A from right to left. The first number you hit that has NO currency (EUR/GBP/USD) and NO unit (EA/SET/KG) after it is the Amount.

- Currency:      From the Col 7 header row (e.g. EUR, GBP).

Data Formatting:
- Dates: always dd-mm-yyyy.
- Decimals: use dot (.) — e.g. 374.00.
- Remove thousands separators (10,000 → 10000).
- Missing fields: use "".

Validation:
- Do not calculate or derive values.
- Do not cross-check Quantity × UnitPrice = Amount.

Output:
- Return only valid JSON — no explanations, markdown, or placeholders.

Document text:
{doc_text}
"""

    call = CustomCall()
    extracted_items = call.send_request("user", prompt)
    extracted_items = extracted_items.replace("```", "").replace("json", "").strip()
    extracted_items = json.loads(extracted_items)

    return extracted_items
