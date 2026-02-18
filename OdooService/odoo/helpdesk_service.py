"""
Donna Odoo - Helpdesk Service
Handles Helpdesk ticket operations.
"""
import logging
from typing import List, Dict, Any, Optional, Union

from .client import OdooClient

logger = logging.getLogger(__name__)

class HelpdeskService:
    """
    Handles Odoo helpdesk.ticket and helpdesk.stage operations.
    """
    
    TICKET_MODEL = "helpdesk.ticket"
    TEAM_MODEL = "helpdesk.team"
    STAGE_MODEL = "helpdesk.stage"
    
    def __init__(self, client: OdooClient):
        self.client = client
        self._team_cache: Dict[str, int] = {}
        self._stage_cache: Dict[str, int] = {}

    def find_team(self, team_name: str) -> Optional[int]:
        """Find Helpdesk Team ID by name."""
        if team_name in self._team_cache:
            return self._team_cache[team_name]
        
        team_ids = self.client.search(self.TEAM_MODEL, [['name', 'ilike', team_name]], limit=1)
        if team_ids:
            self._team_cache[team_name] = team_ids[0]
            return team_ids[0]
        return None

    def find_stage(self, stage_name: str, team_id: Optional[int] = None) -> Optional[int]:
        """Find Stage ID by name, optionally filtered by team."""
        cache_key = f"{stage_name}_{team_id}"
        if cache_key in self._stage_cache:
            return self._stage_cache[cache_key]
        
        domain = [['name', 'ilike', stage_name]]
        if team_id:
            # Stages can be specific to teams or global
            # Usually stages have a team_ids field. If it's empty, it's global? or common?
            # Safe bet: search for name, and check if it's compatible
            pass 
            
        stage_ids = self.client.search(self.STAGE_MODEL, domain, limit=1)
        if stage_ids:
            self._stage_cache[cache_key] = stage_ids[0]
            return stage_ids[0]
        return None

    def create_ticket(
        self,
        name: str,
        team_name: Optional[str] = None,
        description: Optional[str] = None,
        priority: str = "1", # 0=Low, 1=Medium, 2=High, 3=Urgent
        partner_email: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> int:
        """
        Create a new Helpdesk Ticket.
        
        Args:
            name: Ticket Subject/Title
            team_name: Name of the Helpdesk Team (e.g., 'Internal', 'Customer Support')
            description: Ticket description/body
            priority: Priority level (0, 1, 2, 3)
            partner_email: Email of the customer/requester (to link partner)
            tags: List of tags (not fully implemented yet)
            
        Returns:
            Created Ticket ID
        """
        values = {
            'name': name,
            'priority': priority
        }
        
        if team_name:
            team_id = self.find_team(team_name)
            if team_id:
                values['team_id'] = team_id
            else:
                logger.warning(f"⚠️ Helpdesk team '{team_name}' not found. Using default.")

        if description:
            values['description'] = description # Note: description is often HTML

        if partner_email:
            # Try to find partner by email
            partner_ids = self.client.search('res.partner', [['email', '=', partner_email]], limit=1)
            if partner_ids:
                values['partner_id'] = partner_ids[0]
                values['partner_email'] = partner_email

        logger.info(f"🎫 Creating Helpdesk Ticket: {name}")
        return self.client.create(self.TICKET_MODEL, values)

    def get_ticket(self, ticket_id: int) -> Dict[str, Any]:
        """Read full ticket details."""
        data = self.client.read(self.TICKET_MODEL, [ticket_id], [])
        return data[0] if data else {}

    def search_tickets(self, domain: List, limit: int = 10) -> List[Dict[str, Any]]:
        """Search for tickets."""
        return self.client.search_read(
            self.TICKET_MODEL, 
            domain, 
            ['name', 'team_id', 'stage_id', 'priority', 'user_id', 'partner_id'], 
            limit=limit
        )

    def update_ticket_stage(self, ticket_id: int, stage_name: str) -> bool:
        """Move ticket to a new stage."""
        stage_id = self.find_stage(stage_name)
        if not stage_id:
            logger.error(f"❌ Stage '{stage_name}' not found.")
            return False
            
        logger.info(f"🔄 Moving ticket {ticket_id} to stage '{stage_name}'")
        return self.client.write(self.TICKET_MODEL, [ticket_id], {'stage_id': stage_id})

    def add_note(self, ticket_id: int, note: str, internal: bool = True) -> bool:
        """Add a comment/note to the ticket chatter."""
        subtype = 2 if internal else 1 # 1=Discussion, 2=Note (Verify these based on Odoo version)
        # Actually simplest is using mail.message directly or message_post via execute_kw if model supports it
        # But our client 'create' for mail.message is robust.
        
        values = {
            'body': note,
            'model': self.TICKET_MODEL,
            'res_id': ticket_id,
            'message_type': 'comment',
            'subtype_id': 2 if internal else 1
        }
        logger.info(f"📝 Adding {'internal ' if internal else ''}note to ticket {ticket_id}")
        return self.client.create('mail.message', values) > 0

    def get_ticket_messages(self, ticket_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get messages/emails associated with the ticket.
        
        Args:
            ticket_id: The ID of the helpdesk ticket.
            limit: Max number of messages to return.
            
        Returns:
            List of message dictionaries (date, author, body, type).
        """
        domain = [
            ('res_id', '=', ticket_id),
            ('model', '=', self.TICKET_MODEL),
            ('message_type', 'in', ['email', 'comment', 'notification'])
        ]
        
        # We want the newest messages? Usually Odoo returns sorted by ID (creation)
        # Check if we need to sort.
        
        # Retrieve relevant fields
        fields = ['date', 'email_from', 'author_id', 'message_type', 'subtype_id', 'body', 'subject', 'attachment_ids']
        
        
        # Sort by date descending (newest first)
        messages = self.client.search_read(
            'mail.message', 
            domain, 
            fields, 
            limit=limit,
            order='date desc'
        )
        
        return messages

    def get_message_attachments(self, message_id: int) -> List[Dict[str, Any]]:
        """
        Get attachments for a specific message.
        """
        # 1. Get message to find attachment IDs
        message = self.client.read('mail.message', [message_id], ['attachment_ids'])[0]
        attachment_ids = message.get('attachment_ids', [])
        
        if not attachment_ids:
            return []
            
        # 2. Read attachment details (datas field contains base64 content)
        # Note: 'datas' is the field for file content in Odoo 11+ (before it was 'db_datas' or stored on disk)
        # Sometimes it's 'raw', but via XMLRPC 'datas' is standard base64 strings.
        
        attachments = self.client.read('ir.attachment', attachment_ids, ['name', 'datas', 'mimetype', 'file_size'])
        return attachments
