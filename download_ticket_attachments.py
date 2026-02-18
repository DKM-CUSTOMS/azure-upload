from OdooService.config import Config
from OdooService.odoo.client import OdooClient
from OdooService.odoo.helpdesk_service import HelpdeskService
import logging
import base64
import os

logging.basicConfig(level=logging.INFO)

def download_attachments():
    print("\n" + "="*60)
    print("📎 Downloading Attachments from Ticket #128")
    print("="*60)
    
    # Authenticate
    client = OdooClient(Config.ODOO_URL, Config.ODOO_DB, Config.ODOO_USERNAME, Config.ODOO_API_KEY)
    client.authenticate()
    
    service = HelpdeskService(client)
    ticket_id = 128
    
    # 1. Get Messages
    print(f"📧 Fetching messages for ticket {ticket_id}...")
    messages = service.get_ticket_messages(ticket_id)
    
    # Filter for messages with attachments
    msgs_with_attachments = [m for m in messages if m.get('attachment_ids')]
    
    print(f"✅ Found {len(msgs_with_attachments)} messages with attachments.")
    
    if not msgs_with_attachments:
        print("❌ No attachments found.")
        return

    # Create download folder
    output_dir = "downloaded_attachments"
    os.makedirs(output_dir, exist_ok=True)

    # 2. Iterate and Download
    for msg in msgs_with_attachments:
        msg_id = msg['id']
        subject = msg.get('subject') or "No Subject"
        print(f"\n📥 Processing Message [{msg_id}]: {subject}")
        
        attachments = service.get_message_attachments(msg_id)
        
        for att in attachments:
            name = att['name']
            size = att['file_size']
            ctype = att['mimetype']
            content_b64 = att['datas']
            
            print(f"   📎 Found: {name} ({size} bytes, {ctype})")
            
            if content_b64:
                # Decode and Save
                try:
                    file_data = base64.b64decode(content_b64)
                    file_path = os.path.join(output_dir, f"{msg_id}_{name}")
                    
                    with open(file_path, "wb") as f:
                        f.write(file_data)
                        
                    print(f"   ✅ Saved to: {file_path}")
                except Exception as e:
                    print(f"   ❌ Failed to save: {e}")
            else:
                print("   ⚠️ No content (datas) returned.")

    print("\n✅ Done.")

if __name__ == "__main__":
    download_attachments()
