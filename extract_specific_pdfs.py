import zipfile
import os
import shutil

def process_zip(zip_name, target_folder):
    # Ensure target folder exists
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        print(f"Created folder: {target_folder}")

    if not os.path.exists(zip_name):
        print(f"Error: Zip file '{zip_name}' not found.")
        return

    extracted_count = 0
    try:
        with zipfile.ZipFile(zip_name, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                filename = file_info.filename
                # Check for _FAC_ and .pdf extension
                if '_FAC_' in filename and filename.lower().endswith('.pdf'):
                    # Extract to target folder (flattening structure if needed)
                    base_name = os.path.basename(filename)
                    if not base_name: # skip directories
                        continue
                    
                    target_path = os.path.join(target_folder, base_name)
                    with zip_ref.open(file_info) as source, open(target_path, 'wb') as target:
                        shutil.copyfileobj(source, target)
                    
                    print(f"Extracted: {base_name}")
                    extracted_count += 1

        if extracted_count > 0:
            print(f"Successfully extracted {extracted_count} PDF(s).")
            # Delete the zip file as requested ("delete everything else")
            os.remove(zip_name)
            print(f"Deleted source zip file: {zip_name}")
        else:
            print("No matching PDFs found with '_FAC_' in the name.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    zip_file = "FW_ new request - S26017990276 - TLLU8002290 __ ANR-SH-004060.zip"
    output_dir = "FAC_PDFS"
    process_zip(zip_file, output_dir)
