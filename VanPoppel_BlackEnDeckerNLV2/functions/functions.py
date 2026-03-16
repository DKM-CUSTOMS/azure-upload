import ast
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
Extract all invoice items from the provided text document(s) and return the results in strict JSON format as specified below.

JSON Structure Requirements:
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
      "Amount": number,    // ALWAYS the LAST numeric value in the row located in the last column (Amount)
      "Currency": "string"
    }}
  ]
}}

Extraction Rules:

No Omissions:
- Extract every line item from all pages of the invoice.
- Do not stop after a fixed number of rows.
- If the invoice spans multiple pages, combine all items into a single JSON array.

Data Formatting:
- Dates: Always use dd-mm-yyyy format.
- Numbers:
  - Use dots (.) for decimals (e.g., 374.00).
  - Remove thousands separators (e.g., 10,000 → 10000).
  - Ensure NetWeight, Quantity, UnitPrice, and Amount are numeric.
- Currency: Always include the 3-letter currency code (e.g., GBP, USD).
- Empty Fields: If a field (e.g., Origin) is missing, use an empty string "".

Column Mapping:
- Description: Use the item description (e.g., MFX Rivet Stainl Steel DH 4.8x30).
- HSCode: Use the commodity code (e.g., 8308200090).
- NetWeight: Extract the individual item weight (e.g., 12.400).
  - Always near "KG", extract the exact one.
  - NetWeight always has 3 digits after the decimal point.
  - If only the total weight is provided, distribute it proportionally by quantity.
- Amount: ALWAYS the LAST numeric value in the row (regardless of column headers). don't mix it up with UnitPrice. the UnitPrice is the numeric value with a currency before the Amount.
  - Example: If the row ends with 0.44, that is the Amount.

Validation:
- Do not calculate or derive values.
- Do not cross-check Quantity × UnitPrice = Amount.
- If the document is unclear, flag ambiguous fields with "".

Output:
- Return only valid JSON — no explanations, notes, or placeholders.
- If unclear, flag ambiguous fields with "".

Example Output:
{{
  "Items": [
    {{
      "InvoiceNumber": "",
      "InvoiceDate": "",
      "Description": "",
      "HSCode": "",
      "Origin": "",
      "NetWeight": 0.000,
      "Quantity": 0,
      "Amount": 0.00,
      "Currency": ""
    }},
    {{
      "InvoiceNumber": "",
      "InvoiceDate": "",
      "Description": "",
      "HSCode": "",
      "Origin": "",
      "NetWeight": 0.000,
      "Quantity": 0,
      "Amount": 0.00,
      "Currency": ""
    }}
  ]
}}

Key Clarifications:
- Amount is always the last numeric value in the row.
- Do not mix it up with UnitPrice.
- UnitPrice is the numeric value with a currency before the Amount.
- Amount is the last numeric value in the row and does not include a currency (currency is in the header).
- Do not put UnitPrice in the Amount field.
- You may check the total amount at the bottom of the invoice to confirm extraction.
- If the invoice has multiple pages, extract all items into a single JSON array.

Document text:
{doc_text}
"""

    call = CustomCall()
    extracted_items = call.send_request("user", prompt)
    extracted_items = extracted_items.replace("```", "").replace("json", "").strip()
    extracted_items = ast.literal_eval(extracted_items)
    
    return extracted_items