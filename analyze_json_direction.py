import json
import os

def analyze_transport_direction():
    file_path = 'TEST.JSON'
    
    if not os.path.exists(file_path):
        print(f"❌ File {file_path} not found.")
        return

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return

    print(f"Total Records: {len(data)}")
    print("-" * 80)
    print(f"{'DOSSNR_ID':<12} | {'REF':<20} | {'DIR':<5} | {'LOAD':<15} | {'DEST':<15} | {'TYPE'}")
    print("-" * 80)

    export_count = 0
    import_count = 0
    other_count = 0

    for item in data:
        doss_id = item.get('DOSSNR_ID', 'N/A')
        ref = item.get('REFERENCE', 'N/A')
        direction = item.get('TRANSPORT_DIRECTION', '?')
        load_port = str(item.get('CTR_PORT_LOADING', '')) + " " + str(item.get('PORT_LOADING_NAME', ''))
        dest_port = str(item.get('CTR_PORT_DESTINATION', '')) + " " + str(item.get('PORT_DESTINATION_NAME', ''))
        rec_type = item.get('RECORD_TYPE', '')

        # Heuristic for Export vs Import
        # Usually: 
        # Export: Loading Country = Home Country (e.g., BE), Direction = O (Outbound)
        # Import: Destination Country = Home Country (e.g., BE), Direction = I (Inbound)
        
        type_str = "UNKNOWN"
        if direction == 'O':
            type_str = "EXPORT 🚢"
            export_count += 1
        elif direction == 'I':
            type_str = "IMPORT 📥"
            import_count += 1
        else:
            other_count += 1

        print(f"{doss_id:<12} | {ref:<20} | {direction:<5} | {load_port[:15]:<15} | {dest_port[:15]:<15} | {type_str}")

    print("-" * 80)
    print(f"Summary: Exports: {export_count}, Imports: {import_count}, Others: {other_count}")

if __name__ == "__main__":
    analyze_transport_direction()
