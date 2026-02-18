import socket
import xmlrpc.client
import logging

# Set timeout for socket operations
socket.setdefaulttimeout(15)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
ODOO_URL = "https://dkm-customs.odoo.com"
ODOO_DB = "vva-onniti-dkm-main-20654023"
ODOO_USERNAME = "anas.benabbou@dkm-customs.com"
ODOO_API_KEY = "a3d96d0d41b4d1ac3ab1d5cc91d33e7fc0611946"

def test_connection():
    try:
        logger.info(f"Connecting to {ODOO_URL}...")
        
        # 1. Check DB Availability
        try:
            db_sock = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/db")
            dbs = db_sock.list()
            logger.info(f"Available Databases: {dbs}")
            if ODOO_DB not in dbs:
                logger.warning(f"⚠️ configured database '{ODOO_DB}' not found in server list!")
        except Exception as e:
            logger.warning(f"Could not list databases (this is normal for some SaaS/Sh instances): {e}")

        # 2. Check Version
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        version = common.version()
        logger.info(f"Odoo Version: {version}")
        
        # 3. Authenticate
        logger.info(f"Authenticating as {ODOO_USERNAME} on DB '{ODOO_DB}'...")
        # authenticate(db, login, password, user_agent_env)
        uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_API_KEY, {})
        
        if uid:
            logger.info(f"Authentication Successful! UID: {uid}")
            
            # Test object access
            models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
            
            # Try to fetch stages as requested in the other script
            logger.info("Fetching project stages...")
            stage_ids = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'project.task.type', 'search', [[]], {'limit': 5}
            )
            
            stages = models.execute_kw(
                ODOO_DB, uid, ODOO_API_KEY,
                'project.task.type', 'read', [stage_ids], {'fields': ['name', 'sequence']}
            )
            
            for stage in stages:
                print(f"Stage: {stage['name']} (Seq: {stage['sequence']})")
                
        else:
            logger.error("Authentication Failed: uid is None or False")

    except Exception as e:
        logger.error(f"Connection Error: {e}")

if __name__ == "__main__":
    test_connection()
