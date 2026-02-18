import json
import os

def analyze_fields():
    file_path = 'TEST.JSON'
    
    if not os.path.exists(file_path):
        print(f"❌ File {file_path} not found.")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if isinstance(data, dict):
            if "Table1" in data:
                data = data["Table1"]
            else:
                for key, value in data.items():
                    if isinstance(value, list):
                        data = value
                        break
        elif not isinstance(data, list):
            return

    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return

    print(f"Total Records: {len(data)}")
    
    export_count = 0
    import_count = 0
    unknown_count = 0
    
    print("-" * 120)
    print(f"{'DOSSNR':<8} | {'REF':<15} | {'STATUS':<8} | {'REASON / CLUES'}")
    print("-" * 120)

    for item in data:
        status = "UNKNOWN"
        reason = ""
        
        # 1. NEW LOGIC: Check Consignee/Shipper (Strongest Signal)
        # Note: JSON keys are 'ADDR_CONSIGEE' (missing 'N') and 'ADDR_SHIPPER'
        shipper = str(item.get('ADDR_SHIPPER', '')).upper()
        consignee = str(item.get('ADDR_CONSIGEE', '')).upper()
        
        if "SOUDAL" in consignee:
            status = "IMPORT"
            reason = "Consignee ADDR contains SOUDAL"
            import_count += 1
        elif "SOUDAL" in shipper:
            status = "EXPORT"
            reason = "Shipper ADDR contains SOUDAL"
            export_count += 1
            
        # 2. Check Text Keywords (Fallback if status is still UNKNOWN)
        if status == "UNKNOWN":
            text_content = (
                str(item.get('BL_NR', '')) + " " + 
                str(item.get('MASTER_BL_NR', '')) + " " + 
                str(item.get('REF_BILL_TO', '')) + " " +
                str(item.get('INFORMATION', ''))
            ).upper()
            
            if "EXPORT" in text_content:
                status = "EXPORT"
                reason = "Keyword 'EXPORT' found in text"
                export_count += 1
            elif "IMPORT" in text_content:
                status = "IMPORT"
                reason = "Keyword 'IMPORT' found in text"
                import_count += 1

        # 3. Check Ports (Last Resort)
        if status == "UNKNOWN":
            load_ctr = str(item.get('CTR_PORT_LOADING', '')).strip()
            dest_ctr = str(item.get('CTR_PORT_DESTINATION', '')).strip()
            
            if load_ctr == "BE":
                status = "EXPORT"
                reason = "Load Port is BE (Belgium)"
                export_count += 1
            elif dest_ctr == "BE":
                status = "IMPORT"
                reason = "Dest Port is BE (Belgium)"
                import_count += 1

        if status == "UNKNOWN":
            unknown_count += 1
            reason = "No indicators found"
            
        doss_id = str(item.get('DOSSNR_ID', ''))
        ref = str(item.get('REFERENCE', ''))
        
        print(f"{doss_id:<8} | {ref:<15} | {status:<8} | {reason}")

    print("-" * 120)
    print(f"Summary: Exports: {export_count}, Imports: {import_count}, Unknowns: {unknown_count}")

if __name__ == "__main__":
    analyze_fields()
