import fitz

def debug_pdf_blocks(pdf_path):
    doc = fitz.open(pdf_path)
    for page_num in range(len(doc)):
        page = doc[page_num]
        blocks = page.get_text("blocks")
        print(f"--- Page {page_num + 1} ---")
        for b in blocks:
            print(f"Block: {b[4]}")
    doc.close()

if __name__ == "__main__":
    pdf_path = r"c:\Users\pc\Desktop\Projects\Azure funcitons\testAzure\FAC_PDFS\51019251_FAC_0005477506_8326723277.pdf"
    debug_pdf_blocks(pdf_path)
