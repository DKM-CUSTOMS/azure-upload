from OdooService.config import Config
from OdooService.odoo.client import OdooClient
from OdooService.odoo.helpdesk_service import HelpdeskService
import logging

logging.basicConfig(level=logging.INFO)

def read_accepted_ticket():
    print("\n" + "="*60)
    print("📖 Reading First Ticket in 'Accepted' Stage")
    print("="*60)
    
    # Authenticate
    client = OdooClient(Config.ODOO_URL, Config.ODOO_DB, Config.ODOO_USERNAME, Config.ODOO_API_KEY)
    client.authenticate()
    
    service = HelpdeskService(client)
    
    # 1. Find 'Accepted' Stage
    stage_id = service.find_stage("Accepted")
    if not stage_id:
        print("❌ Stage 'Accepted' not found!")
        return

    print(f"✅ Found 'Accepted' Stage ID: {stage_id}")

    # 2. Find First Ticket
    tickets = service.search_tickets([['stage_id', '=', stage_id]], limit=1)
    
    if not tickets:
        print("⚠️ No tickets found in 'Accepted' stage.")
        return

    ticket = tickets[0]
    print(f"\n🎫 Ticket Found: [{ticket['id']}] {ticket['name']}")
    print(f"   Priority: {ticket['priority']}")
    print(f"   Assigned to: {ticket['user_id'][1] if ticket['user_id'] else 'Unassigned'}")
    
    # 3. Get Messages
    print("\n📧 Messages:")
    messages = service.get_ticket_messages(ticket['id'])
    
    if not messages:
        print("   (No messages found)")
    
    for msg in messages:
        date = msg['date']
        author = msg['author_id'][1] if msg['author_id'] else msg['email_from'] or "Unknown"
        m_type = msg['message_type']
        subject = msg['subject'] or "(No Subject)"
        
        # Clean body for printing (simple strip)
        body = msg['body'] or ""
        # Remove HTML tags ideally, but for now just print raw or stripped
        import re
        clean_body = re.sub('<[^<]+?>', '', body).strip()
        
        print("-" * 50)
        print(f"Date:   {date}")
        print(f"From:   {author} ({m_type})")
        print(f"Subject: {subject}")
        print(f"Body:   {clean_body[:500]}..." if len(clean_body) > 500 else f"Body:   {clean_body}")
        print("-" * 50)

if __name__ == "__main__":
    read_accepted_ticket()
