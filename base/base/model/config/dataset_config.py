"""Tenant dataset config"""
from pydantic import BaseModel


class TenantDatasetConfig(BaseModel):
    """
    Represents a list of tenants that should use a specific dataset
    """
    name: str
    tenants: list[str]


class DatasetConfig(BaseModel):
    """
    Config for dataset mappings containing default dataset and tag
    """

    create_dataset_for: list[TenantDatasetConfig] = []
    default_dataset: str
    tag: str
