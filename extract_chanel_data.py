import fitz
import pandas as pd
import os
import re

def clean_number(text):
    if not text:
        return 0.0
    # Remove spaces and thousands separators (.) and replace decimal separator (,) with (.)
    text = text.replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(text)
    except:
        return 0.0

def extract_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    all_items = []
    
    invoice_no = ""
    invoice_date = ""
    
    # Try to find invoice header info on first page
    first_page_text = doc[0].get_text("text")
    inv_match = re.search(r"INVOICE\s+(\d+)", first_page_text)
    if inv_match:
        invoice_no = inv_match.group(1)
    
    date_match = re.search(r"Neuilly, le\s+(\d{2}\.\d{2}\.\d{4})", first_page_text)
    if date_match:
        invoice_date = date_match.group(1)

    for page in doc:
        words = page.get_text("words")
        # Sort words by y (with tolerance) then x
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
            
            # Start/End markers for the page
            if "Items to be charged" in line_text or "Référence Quantité" in line_text:
                in_items_area = True
                continue
            if "Total invoiced goods" in line_text or "Net amount" in line_text:
                in_items_area = False
                continue
                
            if in_items_area:
                # Row starts with 7-digit Ref
                match = re.search(r"^(\d{7})\s+(.*)$", line_text)
                if match:
                    ref = match.group(1)
                    rest = match.group(2)
                    parts = rest.split()
                    
                    if len(parts) >= 5:
                        # Fixed ends: Origin (-1), CommCode (-2)
                        origin = parts[-1]
                        comm_code = parts[-2]
                        
                        # CommCode should be numeric and ~10 digits
                        if not re.match(r"^\d{8,12}$", comm_code):
                            # Maybe comm code and origin are swapped or origin has spaces?
                            # Heuristic: if -1 is digits, it might be comm code
                            if re.match(r"^\d{10}$", parts[-1]):
                                comm_code = parts[-1]
                                origin = "" # Unknown
                        
                        # Reverse search for Amount and Price (look for decimals)
                        # We need at least Qty, Desc, Price, Amount
                        qty = parts[0]
                        numeric_candidates = []
                        for i in range(1, len(parts)-1): # Between qty and comm code
                            if re.search(r"\d+[\.,]\d+", parts[i]):
                                numeric_candidates.append(i)
                        
                        # Typically: Qty [Desc] Price Amount [Vol%] Comm Origin
                        # If we have 2 candidates: Price, Amount
                        # If we have 3: Price, Amount, Vol%
                        
                        price = "0"
                        amount = "0"
                        vol = ""
                        desc_end_idx = 1
                        
                        if len(numeric_candidates) >= 2:
                            price = parts[numeric_candidates[-2]]
                            amount = parts[numeric_candidates[-1]]
                            desc_end_idx = numeric_candidates[-2]
                            # Check if there's a third one for Vol%
                            if len(numeric_candidates) >= 3:
                                # Vol% is after Amount in the layout header (%Vol is after Montant)
                                # Wait, header says: Montant (EUR) %Vol Commodity code
                                # So Vol is between Amount and CommCode.
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
                            "Source PDF": os.path.basename(pdf_path)
                        })

    doc.close()
    return all_items

def main():
    pdf_dir = r"c:\Users\pc\Desktop\Projects\Azure funcitons\testAzure\FAC_PDFS"
    all_data = []
    
    if not os.path.exists(pdf_dir):
        print(f"Directory {pdf_dir} not found.")
        return

    files = [f for f in os.listdir(pdf_dir) if f.lower().endswith(".pdf")]
    print(f"Found {len(files)} PDFs.")
    
    for file in files:
        path = os.path.join(pdf_dir, file)
        try:
            items = extract_from_pdf(path)
            all_data.extend(items)
        except Exception as e:
            print(f"Error processing {file}: {e}")
            
    if all_data:
        df = pd.DataFrame(all_data)
        output_file = "Chanel_Invoices_Extraction_v2.xlsx"
        df.to_excel(output_file, index=False)
        print(f"Extraction complete. Saved to {output_file}")
        print(f"Total rows extracted: {len(df)}")
    else:
        print("No data extracted.")

if __name__ == "__main__":
    main()
