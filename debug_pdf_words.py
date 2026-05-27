import fitz

def debug_pdf_words(pdf_path):
    doc = fitz.open(pdf_path)
    page = doc[0]
    words = page.get_text("words")
    # Sort words by y coordinate then x
    words.sort(key=lambda w: (w[1], w[0]))
    
    current_y = -1
    line = []
    for w in words:
        if abs(w[1] - current_y) > 2:
            if line:
                print(" ".join([x[4] for x in line]))
            line = [w]
            current_y = w[1]
        else:
            line.append(w)
    if line:
        print(" ".join([x[4] for x in line]))
    doc.close()

if __name__ == "__main__":
    pdf_path = r"c:\Users\pc\Desktop\Projects\Azure funcitons\testAzure\FAC_PDFS\51019251_FAC_0005477506_8326723277.pdf"
    debug_pdf_words(pdf_path)
