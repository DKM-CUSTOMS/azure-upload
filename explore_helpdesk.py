from OdooService.odoo.client import OdooClient
from OdooService.config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def helpdesk_explorer():
    print("\n" + "="*60)
    print("📡 Exploring Odoo Helpdesk Module")
    print("="*60)
    
    try:
        client = OdooClient(Config.ODOO_URL, Config.ODOO_DB, Config.ODOO_USERNAME, Config.ODOO_API_KEY)
        client.authenticate()
        
        # 1. Check if helpdesk module is installed/accessible
        print("\n🔍 Checking available Helpdesk Models...")
        
        # Try to search for Helpdesk Teams first
        try:
            teams = client.search_read('helpdesk.team', [], ['name', 'alias_name'], limit=5)
            print(f"✅ Found {len(teams)} Helpdesk Teams:")
            for t in teams:
                print(f"   - {t['name']} (ID: {t['id']})")
        except Exception as e:
            print(f"❌ Could not access 'helpdesk.team': {e}")
            
        # 2. Get Helpdesk Stages
        print("\n🔍 Fetching Helpdesk Stages (helpdesk.stage)...")
        try:
            stages = client.search_read('helpdesk.stage', [], ['name', 'sequence', 'team_ids'], limit=10)
            print(f"✅ Found {len(stages)} Stages:")
            for s in stages:
                print(f"   - {s['name']} (Seq: {s['sequence']})")
        except Exception as e:
            print(f"❌ Could not access 'helpdesk.stage': {e}")

        # 3. Get Recent Tickets
        print("\n🔍 Fetching Recent Tickets (helpdesk.ticket)...")
        try:
            # Get fields first to see what's available
            # fields = client.fields_get('helpdesk.ticket', ['name', 'stage_id', 'user_id', 'priority'])
            
            tickets = client.search_read('helpdesk.ticket', [], 
                                       ['name', 'stage_id', 'user_id', 'priority', 'create_date'], 
                                       limit=5)
            print(f"✅ Found {len(tickets)} Tickets:")
            for t in tickets:
                stage = t['stage_id'][1] if t['stage_id'] else "No Stage"
                user = t['user_id'][1] if t['user_id'] else "Unassigned"
                print(f"   - [{t['id']}] {t['name']} | Stage: {stage} | Assigned: {user}")
                
        except Exception as e:
            print(f"❌ Could not access 'helpdesk.ticket': {e}")

    except Exception as e:
        logger.error(f"Connection Failed: {e}")

if __name__ == "__main__":
    helpdesk_explorer()
