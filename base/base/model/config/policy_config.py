"""Policy Config"""
from pydantic import BaseModel


class PolicyConfig(BaseModel):
    """
    Config for policy mapping of tenants
    """
    default_policy_document: str
    policy_document_per_tenant: dict[str, str] = {}
