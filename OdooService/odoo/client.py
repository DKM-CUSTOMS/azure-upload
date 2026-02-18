"""
Donna Odoo - Enhanced Client
XML-RPC client for Odoo with extended operations.
"""
import xmlrpc.client
import logging
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class OdooClient:
    """
    Enhanced Odoo XML-RPC client.
    
    Provides:
    - Authentication
    - CRUD operations
    - Search operations
    - Many2many field handling
    """
    
    def __init__(self, url: str, db: str, username: str, api_key: str):
        """
        Initialize Odoo client.
        
        Args:
            url: Odoo instance URL
            db: Database name
            username: Username/email
            api_key: API key or password
        """
        self.url = url.rstrip('/')
        self.db = db
        self.username = username
        self.api_key = api_key
        self.uid = None
        self.models = None
    
    def authenticate(self) -> int:
        """
        Authenticate with Odoo.
        
        Returns:
            User ID (uid)
            
        Raises:
            Exception: If authentication fails
        """
        logger.info(f"🔐 Authenticating with Odoo: {self.url}")
        
        common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.uid = common.authenticate(self.db, self.username, self.api_key, {})
        
        if not self.uid:
            raise Exception("Odoo Authentication Failed")
        
        self.models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        logger.info(f"✅ Authenticated as user {self.uid}")
        
        return self.uid
    
    def _ensure_authenticated(self):
        """Ensure client is authenticated."""
        if not self.uid or not self.models:
            self.authenticate()
    
    def create(self, model: str, values: Dict[str, Any]) -> int:
        """
        Create a new record.
        
        Args:
            model: Odoo model name (e.g., 'project.task')
            values: Field values dictionary
            
        Returns:
            Created record ID
        """
        self._ensure_authenticated()
        
        logger.info(f"📝 Creating {model}: {list(values.keys())}")
        
        record_id = self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'create', [values]
        )
        
        logger.info(f"✅ Created {model} ID: {record_id}")
        return record_id
    
    def search(
        self, 
        model: str, 
        domain: List, 
        limit: Optional[int] = None
    ) -> List[int]:
        """
        Search for records.
        
        Args:
            model: Odoo model name
            domain: Search domain (list of conditions)
            limit: Optional max records to return
            
        Returns:
            List of record IDs
        """
        self._ensure_authenticated()
        
        kwargs = {}
        if limit:
            kwargs['limit'] = limit
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'search', [domain], kwargs
        )
    
    def read(
        self, 
        model: str, 
        ids: List[int], 
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Read records by ID.
        
        Args:
            model: Odoo model name
            ids: List of record IDs
            fields: Optional list of fields to read
            
        Returns:
            List of record dictionaries
        """
        self._ensure_authenticated()
        
        kwargs = {}
        if fields:
            kwargs['fields'] = fields
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'read', [ids], kwargs
        )
    
    def search_read(
        self, 
        model: str, 
        domain: List, 
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        order: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search and read in one call.
        
        Args:
            model: Odoo model name
            domain: Search domain
            fields: Optional fields to read
            limit: Optional max records
            order: Optional sort order
            
        Returns:
            List of record dictionaries
        """
        self._ensure_authenticated()
        
        kwargs = {}
        if fields:
            kwargs['fields'] = fields
        if limit:
            kwargs['limit'] = limit
        if order:
            kwargs['order'] = order
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'search_read', [domain], kwargs
        )
    
    def write(self, model: str, ids: List[int], values: Dict[str, Any]) -> bool:
        """
        Update records.
        
        Args:
            model: Odoo model name
            ids: List of record IDs to update
            values: New field values
            
        Returns:
            True if successful
        """
        self._ensure_authenticated()
        
        logger.info(f"✏️ Updating {model} IDs {ids}: {list(values.keys())}")
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'write', [ids, values]
        )
    
    def unlink(self, model: str, ids: List[int]) -> bool:
        """
        Delete records.
        
        Args:
            model: Odoo model name
            ids: List of record IDs to delete
            
        Returns:
            True if successful
        """
        self._ensure_authenticated()
        
        logger.warning(f"🗑️ Deleting {model} IDs {ids}")
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'unlink', [ids]
        )
    
    def fields_get(
        self, 
        model: str, 
        attributes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get model field definitions.
        
        Args:
            model: Odoo model name
            attributes: Optional list of attributes to return
            
        Returns:
            Dictionary of field definitions
        """
        self._ensure_authenticated()
        
        kwargs = {}
        if attributes:
            kwargs['attributes'] = attributes
        
        return self.models.execute_kw(
            self.db, self.uid, self.api_key,
            model, 'fields_get', [], kwargs
        )
