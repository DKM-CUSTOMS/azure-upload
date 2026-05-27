import fitz
import sys

def debug_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text("text")
        print(f"--- Page {page_num + 1} ---")
        print(text)
    doc.close()

if __name__ == "__main__":
    pdf_path = r"c:\Users\pc\Desktop\Projects\Azure funcitons\testAzure\FAC_PDFS\51019251_FAC_0005477506_8326723277.pdf"
    debug_pdf(pdf_path)
