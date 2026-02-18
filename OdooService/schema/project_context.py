"""
Donna Schema - Project Context
Enriched context for project creation after LLM processing.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from .email_payload import EmailPayload
from .odoo_fields import ProjectType, DataSource, Priority


@dataclass
class ExtractedData:
    """
    Data extracted by LLM from email/attachments.
    """
    client: str
    flow_type: str
    keywords: List[str] = field(default_factory=list)
    suggested_task_name: Optional[str] = None
    suggested_description: Optional[str] = None
    logic_app_name: Optional[str] = None
    azure_function_name: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtractedData":
        """Create from LLM response dictionary."""
        return cls(
            client=data.get('client', 'Unknown'),
            flow_type=data.get('flow_type', 'Unknown'),
            keywords=data.get('keywords', []),
            suggested_task_name=data.get('task_name'),
            suggested_description=data.get('description'),
            logic_app_name=data.get('logic_app'),
            azure_function_name=data.get('azure_function'),
            raw_response=data
        )

    @property
    def triggers(self) -> List[str]:
        return self.raw_response.get('triggers', []) if self.raw_response else []
    
    @property
    def actions(self) -> List[str]:
        return self.raw_response.get('actions', []) if self.raw_response else []
    
    @property
    def integrations(self) -> List[str]:
        return self.raw_response.get('integrations', []) if self.raw_response else []
    
    @property
    def schedule(self) -> Optional[str]:
        return self.raw_response.get('schedule') if self.raw_response else None
        
    @property
    def input_format(self) -> Optional[str]:
        return self.raw_response.get('input_format') if self.raw_response else None
        
    @property
    def output_format(self) -> str:
        return self.raw_response.get('output_format', 'Excel') if self.raw_response else 'Excel'
        
    @property
    def logic_app(self) -> Optional[str]:
        return self.logic_app_name
        
    @property
    def azure_function(self) -> Optional[str]:
        return self.azure_function_name


@dataclass
class ProjectContext:
    """
    Complete context for creating an Odoo project/task.
    Built by the Brain after processing.
    """
    email: EmailPayload
    extracted_data: ExtractedData
    project_type: ProjectType
    tags: List[str] = field(default_factory=list)
    data_sources: List[DataSource] = field(default_factory=list)
    priority: Priority = Priority.MEDIUM
    
    @property
    def task_name(self) -> str:
        """Generate task name from extracted data."""
        if self.extracted_data.suggested_task_name:
            return self.extracted_data.suggested_task_name
        
        client = self.extracted_data.client
        flow = self.extracted_data.flow_type
        return f"{client} – {flow}"
    
    @property
    def task_description(self) -> str:
        """Generate task description."""
        if self.extracted_data.suggested_description:
            base = self.extracted_data.suggested_description
        else:
            base = self._generate_description()
        
        # Append email metadata
        metadata = f"""
---
**Email Metadata:**
- From: {self.email.from_address}
- Subject: {self.email.subject}
- Received: {self.email.received_at}
- Attachments: {', '.join(self.email.attachment_names) if self.email.attachment_names else 'None'}
"""
        return f"{base}\n{metadata}"
    
    def _generate_description(self) -> str:
        """Generate description from extracted data."""
        data = self.extracted_data
        att_types = ', '.join(self.email.attachment_types) if self.email.attachment_types else 'None'
        
        return f"""
**Project Type:** {self.project_type.value}

**Client:** {data.client}
**Flow Type:** {data.flow_type}

**Data Sources:** {', '.join([ds.value for ds in self.data_sources])}
**Attachment Types:** {att_types}

**Logic App:** {data.logic_app_name or 'To be created'}
**Azure Function:** {data.azure_function_name or 'To be created'}
""".strip()
    
    @property
    def odoo_project_name(self) -> str:
        """Get the Odoo project name based on project type."""
        from .odoo_fields import OdooProject
        return OdooProject.from_project_type(self.project_type).value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "project_type": self.project_type.value,
            "task_name": self.task_name,
            "task_description": self.task_description,
            "tags": self.tags,
            "data_sources": [ds.value for ds in self.data_sources],
            "priority": self.priority.value,
            "client": self.extracted_data.client,
            "flow_type": self.extracted_data.flow_type,
            "email_from": self.email.from_address,
            "email_subject": self.email.subject
        }
