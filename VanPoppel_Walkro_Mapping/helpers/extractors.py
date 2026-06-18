import json
import logging
from AI_agents.Mistral.MistralDocumentQA import MistralDocumentQA

def extract_clean_excel_from_pdf(base64_pdf: str, filename):
    
    prompt = """
Extract the following information from the provided PDF document and format it into a JSON structure.
Ensure the following:
- All numerical values must be real numbers (convert numbers like "23,32" to 23.32 if it's using ',' as a decimal separator).
- Dates must follow the format YYYY-MM-DD.
- Countries must be represented by their 2-letter ISO codes.
- Remove any units (e.g., 'kg') or symbols from values.
- Omit any fields that are not present in the document.
- Strictly follow the dictionary structure with no additional text or explanation.

Note:
In the source documents, commas (,) are used as decimal separators. For example, "23,32" means 23.32 — parse accordingly.

Extract only the invoice data. Extract the following fields:
- net_weight_kg: from the invoice weight in ton (convert to kg)
- invoice_date: from the invoice date
- license_plate: from Kenteken/Pasnr.
- total_amount: from the total amount
- currency: from the currency used
- description: from the description of the commodity
- package: from the package details (Extract from invoice exp 52 PK)
- date: from the invoice date
- transport_fee: from any transport fee if available
- address: The reciever address we need its always in the right (extract the adress then parse it don't change anything or search for things stuck to whats in the pdf) (this one WALKRO HANDELSONDERNEMING BELGIË NV or Rohum Negrone is the sender don't extract it ). [company name, street, city, postal code, country (in 2-letter ISO code)]
- Order number: from the order number if available
- Relatie number: from the relatie number if available
- Truck number: this value is in the table of the first page example : '70 BTS 8' format: (2 numbers + three letters + number) it's under bezeichnung value.

Dictionary Structure:
invoice_data = {
    "VAT exporter": "<VAT number without dots or spaces>",
    "Transport fee": "<transport fee if available>",
    "Order number": "< Order number fee if available> (always extract it from the second page)",
    "Relatie number": "< Relatie number fee if available> (always extract it from the second page) (only number if a letter or special case is present remove it)",
    "License plate": "<license plate or pass number>",
    "Truck number": "<Truck number if available> (always extract it from the first page)",
    "Address": {
        "Company name": "<name of the company>",
        "Street": "<street and number>",
        "Postcode": "<postcode (MUST extract the 4-5 digit number like '5324' if it is next to the city)>",
        "City": "<city>",
        "Country": "<country code>"
    },
    "Items": [
        {
            "Description": "<description of the commodity>",
            "Net weight": <gross weight in kg as a number>,
            "Invoice value": <invoice value as a number>,
            "Currency": "<currency code>",
            "Invoice date": "<invoice date in YYYY-MM-DD format>",
            "Package": "<package details>",
            "Date": "<date in YYYY-MM-DD format>"
        }
    ]
}

Instructions:
- VAT Exporter: Extract and normalize the VAT number (no spaces or dots).
- Address: Extract Company name, street, postcode (CRITICAL: extract the numerical postcode like '5324' which is usually located next to or above the city), city, and country (as ISO code).
- Transport fee: Extract transport fee if available
- License plate: Kenteken/Pasnr.
- Truck number: Truck number if available (always extract it from the first page)
- Items: Extract the commodity data:
  - Description: Commodity description
  - Net weight: Convert weight from ton to kg
  - Invoice value: Extract as a number (if available)
  - Currency: Extract currency code (if available)
  - Invoice date: From invoice date
  - Package: Package details
  - Date: Invoice date

 Ensure that the extracted data strictly adheres to the JSON format provided above. Output only valid JSON. No additional commentary or explanation.
"""
    
    # Mistral call
    qa = MistralDocumentQA()
    logging.error(f"====== PROMPT GIVEN TO AI FOR {filename} ======\n{prompt}\n==========================================")
    response = qa.ask_document(base64_pdf, prompt, filename=filename)

    # Clean response
    raw = response.replace("```", "").replace("json", "").strip()
    
    parsed = json.loads(raw)
    
    return parsed