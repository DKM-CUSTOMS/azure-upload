import logging
import azure.functions as func
import zipfile
import os
import shutil
import tempfile
import fitz
import pandas as pd
import re
import io

def clean_number(text):
    if not text:
        return 0.0
    text = text.replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(text)
    except:
        return 0.0

def extract_from_pdf_data(pdf_stream, filename):
    doc = fitz.open(stream=pdf_stream, filetype="pdf")
    all_items = []
    
    invoice_no = ""
    invoice_date = ""
    
    first_page_text = doc[0].get_text("text")
    inv_match = re.search(r"INVOICE\s+(\d+)", first_page_text)
    if inv_match:
        invoice_no = inv_match.group(1)
    
    date_match = re.search(r"Neuilly, le\s+(\d{2}\.\d{2}\.\d{4})", first_page_text)
    if date_match:
        invoice_date = date_match.group(1)

    for page in doc:
        words = page.get_text("words")
        words.sort(key=lambda w: (round(w[1], 1), w[0]))
        
        lines = []
        current_y = -1
        current_line = []
        for w in words:
            if abs(w[1] - current_y) > 3:
                if current_line:
                    lines.append(current_line)
                current_line = [w]
                current_y = w[1]
            else:
                current_line.append(w)
        if current_line:
            lines.append(current_line)
            
        in_items_area = False
        for line_words in lines:
            line_text = " ".join([w[4] for w in line_words]).strip()
            
            if "Items to be charged" in line_text or "Référence Quantité" in line_text:
                in_items_area = True
                continue
            if "Total invoiced goods" in line_text or "Net amount" in line_text:
                in_items_area = False
                continue
                
            if in_items_area:
                match = re.search(r"^(\d{7})\s+(.*)$", line_text)
                if match:
                    ref = match.group(1)
                    rest = match.group(2)
                    parts = rest.split()
                    
                    if len(parts) >= 5:
                        origin = parts[-1]
                        comm_code = parts[-2]
                        
                        qty = parts[0]
                        numeric_candidates = []
                        for i in range(1, len(parts)-1):
                            if re.search(r"\d+[\.,]\d+", parts[i]):
                                numeric_candidates.append(i)
                        
                        price = "0"
                        amount = "0"
                        vol = ""
                        desc_end_idx = 1
                        
                        if len(numeric_candidates) >= 2:
                            price = parts[numeric_candidates[-2]]
                            amount = parts[numeric_candidates[-1]]
                            desc_end_idx = numeric_candidates[-2]
                            if len(numeric_candidates) >= 3:
                                vol = parts[numeric_candidates[-1]]
                                amount = parts[numeric_candidates[-2]]
                                price = parts[numeric_candidates[-3]]
                                desc_end_idx = numeric_candidates[-3]
                        
                        description = " ".join(parts[1:desc_end_idx])
                        
                        all_items.append({
                            "Invoice No": invoice_no,
                            "Invoice Date": invoice_date,
                            "Reference": ref,
                            "Quantity": clean_number(qty),
                            "Description": description,
                            "Unit Price": clean_number(price),
                            "Amount": clean_number(amount),
                            "Vol %": vol,
                            "Commodity Code": comm_code,
                            "Origin": origin,
                            "Source PDF": filename
                        })

    doc.close()
    return all_items

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Chanel Invoice Zip request.')

    try:
        req_body = req.get_body()
        if not req_body:
            return func.HttpResponse("Please send a ZIP file in the request body.", status_code=400)

        all_data = []
        with zipfile.ZipFile(io.BytesIO(req_body)) as z:
            pdf_files = [f for f in z.namelist() if '_FAC_' in f and f.lower().endswith('.pdf')]
            
            if not pdf_files:
                return func.HttpResponse("No PDFs with '_FAC_' found in the zip file.", status_code=400)

            for pdf_name in pdf_files:
                with z.open(pdf_name) as f:
                    pdf_data = f.read()
                    items = extract_from_pdf_data(pdf_data, os.path.basename(pdf_name))
                    all_data.extend(items)

        if not all_data:
            return func.HttpResponse("No data could be extracted from found PDFs.", status_code=404)

        df = pd.DataFrame(all_data)
        
        # Save to buffer
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)

        filename = "Chanel_Extraction.xlsx"
        return func.HttpResponse(
            output.read(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)
