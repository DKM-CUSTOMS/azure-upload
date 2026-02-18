import json

def check_port_names():
    with open('TEST.JSON', 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"{'DIR':<5} | {'CTR_LOAD':<10} | {'NAME_LOAD':<20} | {'CTR_DEST':<10} | {'NAME_DEST':<20}")
    print("-" * 80)

    for item in data[:50]: # Check first 50
        d = item.get('TRANSPORT_DIRECTION', '')
        cl = item.get('CTR_PORT_LOADING', '')
        nl = item.get('PORT_LOADING_NAME', '')
        cd = item.get('CTR_PORT_DESTINATION', '')
        nd = item.get('PORT_DESTINATION_NAME', '')
        
        if d == 'I':
             print(f"{d:<5} | {cl:<10} | {nl[:20]:<20} | {cd:<10} | {nd[:20]:<20}")
        if d == 'O':
             print(f"{d:<5} | {cl:<10} | {nl[:20]:<20} | {cd:<10} | {nd[:20]:<20}")

if __name__ == "__main__":
    check_port_names()
