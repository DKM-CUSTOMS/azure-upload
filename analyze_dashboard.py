from OdooService.config import Config
from OdooService.odoo.client import OdooClient
from OdooService.odoo.helpdesk_service import HelpdeskService
import logging

logging.basicConfig(level=logging.INFO)

def analyze_dashboard():
    print("\n" + "="*60)
    print("📊 Odoo Helpdesk Dashboard Analyzer")
    print("="*60)
    
    # 1. Connect
    client = OdooClient(Config.ODOO_URL, Config.ODOO_DB, Config.ODOO_USERNAME, Config.ODOO_API_KEY)
    client.authenticate()
    service = HelpdeskService(client)
    
    team_name = "CMR-FISCAL REPRESENTATION"
    team_id = service.find_team(team_name)
    
    if not team_id:
        print(f"❌ Team '{team_name}' not found.")
        return

    print(f"✅ Connected to Team: {team_name} (ID: {team_id})")

    # 2. Get Stages for this team
    # Note: Stages usually have 'team_ids' containing the team ID, or are global.
    # We'll fetch all helpdesk stages and filter/sort.
    stages = client.search_read('helpdesk.stage', [], ['name', 'sequence', 'team_ids'], order='sequence')
    
    dashboard = {}
    
    print("\n📋 Current Board Status:")
    print("-" * 65)
    print(f"{'STAGE':<30} | {'COUNT':<5} | {'TOP TICKET'}")
    print("-" * 65)
    
    for stage in stages:
        # Check if stage belongs to our team (if team_ids is set)
        if stage['team_ids'] and team_id not in stage['team_ids']:
            continue
            
        stage_id = stage['id']
        stage_name = stage['name']
        
        # Count tickets in this stage for this team
        domain = [
            ('team_id', '=', team_id),
            ('stage_id', '=', stage_id)
        ]
        
        # Get count and top ticket
        tickets = service.search_tickets(domain, limit=1)
        # To get accurate count, we ideally use search_count, but search_read result len is limited by limit.
        # Let's do a separate count or just search IDs.
        count = client.models.execute_kw(
            client.db, client.uid, client.api_key,
            'helpdesk.ticket', 'search_count', [domain]
        )
        
        top_ticket = tickets[0]['name'][:40] + "..." if tickets else ""
        
        print(f"{stage_name:<30} | {count:<5} | {top_ticket}")
        
    print("-" * 65)

if __name__ == "__main__":
    analyze_dashboard()
