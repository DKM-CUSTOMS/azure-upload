import os
import time
import shutil
import zipfile
import fitz
import pandas as pd
import re
import io
from datetime import datetime

# CONFIGURATION
WATCH_DIR = "INCOMING_ZIPS"
PROCESSED_DIR = "PROCESSED_ZIPS"
OUTPUT_DIR = "EXTRACTED_RESULTS"

# Ensure folders exist
for folder in [WATCH_DIR, PROCESSED_DIR, OUTPUT_DIR]:
    if not os.path.exists(folder):
        os.makedirs(folder)

def clean_number(text):
    if not text: return 0.0
    text = text.replace(' ', '').replace('.', '').replace(',', '.')
    try: return float(text)
    except: return 0.0

def extract_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    all_items = []
    invoice_no, invoice_date = "", ""
    
    first_page_text = doc[0].get_text("text")
    inv_match = re.search(r"INVOICE\s+(\d+)", first_page_text)
    if inv_match: invoice_no = inv_match.group(1)
    date_match = re.search(r"Neuilly, le\s+(\d{2}\.\d{2}\.\d{4})", first_page_text)
    if date_match: invoice_date = date_match.group(1)

    for page in doc:
        words = page.get_text("words")
        words.sort(key=lambda w: (round(w[1], 1), w[0]))
        lines, current_y, current_line = [], -1, []
        for w in words:
            if abs(w[1] - current_y) > 3:
                if current_line: lines.append(current_line)
                current_line, current_y = [w], w[1]
            else: current_line.append(w)
        if current_line: lines.append(current_line)
            
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
                    ref, rest = match.group(1), match.group(2)
                    parts = rest.split()
                    if len(parts) >= 5:
                        origin, comm_code = parts[-1], parts[-2]
                        qty = parts[0]
                        numeric_candidates = [i for i, p in enumerate(parts[1:-1], 1) if re.search(r"\d+[\.,]\d+", p)]
                        price, amount, vol, desc_end_idx = "0", "0", "", 1
                        if len(numeric_candidates) >= 2:
                            price, amount, desc_end_idx = parts[numeric_candidates[-2]], parts[numeric_candidates[-1]], numeric_candidates[-2]
                            if len(numeric_candidates) >= 3:
                                vol, amount, price, desc_end_idx = parts[numeric_candidates[-1]], parts[numeric_candidates[-2]], parts[numeric_candidates[-3]], numeric_candidates[-3]
                        description = " ".join(parts[1:desc_end_idx])
                        all_items.append({
                            "Invoice No": invoice_no, "Invoice Date": invoice_date, "Reference": ref,
                            "Quantity": clean_number(qty), "Description": description, "Unit Price": clean_number(price),
                            "Amount": clean_number(amount), "Vol %": vol, "Commodity Code": comm_code, "Origin": origin,
                            "Source PDF": os.path.basename(pdf_path)
                        })
    doc.close()
    return all_items

def process_zip(zip_path):
    print(f"[{datetime.now()}] Processing: {os.path.basename(zip_path)}")
    temp_extract = "temp_batch"
    if os.path.exists(temp_extract): shutil.rmtree(temp_extract)
    os.makedirs(temp_extract)
    
    all_data = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            for f in z.namelist():
                if '_FAC_' in f and f.lower().endswith('.pdf'):
                    target = os.path.join(temp_extract, os.path.basename(f))
                    with z.open(f) as source, open(target, 'wb') as dest:
                        shutil.copyfileobj(source, dest)
                    all_data.extend(extract_from_pdf(target))
        
        if all_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(OUTPUT_DIR, f"Chanel_Data_{timestamp}.xlsx")
            pd.DataFrame(all_data).to_excel(output_file, index=False)
            print(f"  --> Success! Extracted {len(all_data)} items to {output_file}")
        else:
            print("  --> No matching items found.")
            
    except Exception as e:
        print(f"  --> Error: {e}")
    finally:
        if os.path.exists(temp_extract): shutil.rmtree(temp_extract)
        # Move to processed
        shutil.move(zip_path, os.path.join(PROCESSED_DIR, os.path.basename(zip_path)))

if __name__ == "__main__":
    print(f"Starting Folder Watcher for Chanel Zips...")
    print(f"Drop your zip files into: {WATCH_DIR}")
    try:
        while True:
            zips = [f for f in os.listdir(WATCH_DIR) if f.lower().endswith(".zip")]
            for z in zips:
                process_zip(os.path.join(WATCH_DIR, z))
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nStopping Watcher.")
