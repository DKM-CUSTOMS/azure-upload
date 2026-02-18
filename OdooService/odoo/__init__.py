"""
Donna Odoo Module
Odoo integration for projects, tasks, tags, and attachments.
"""
from .client import OdooClient
from .tag_service import TagService
from .project_service import ProjectService
from .helpdesk_service import HelpdeskService

__all__ = ["OdooClient", "TagService", "ProjectService", "HelpdeskService"]
