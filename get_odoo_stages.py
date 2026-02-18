from OdooService.odoo.client import OdooClient
from OdooService.config import Config
import logging

logging.basicConfig(level=logging.INFO)

def get_stages():
    print("📡 Connecting to Odoo to fetch Kanban Stages...")
    client = OdooClient(Config.ODOO_URL, Config.ODOO_DB, Config.ODOO_USERNAME, Config.ODOO_API_KEY)
    client.authenticate()
    
    # Fetch all project stages
    stage_ids = client.search('project.task.type', [])
    stages = client.read('project.task.type', stage_ids, ['name', 'sequence', 'project_ids'])
    
    print("\n" + "="*50)
    print(f"{'ID':<5} | {'NAME':<25} | {'SEQUENCE':<10}")
    print("-" * 50)
    for s in stages:
        print(f"{s['id']:<5} | {s['name']:<25} | {s['sequence']:<10}")
    print("="*50)

if __name__ == "__main__":
    get_stages()
