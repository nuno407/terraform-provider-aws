""" pydantic model config """
from pydantic import BaseModel


class ConfiguredBaseModel(BaseModel):
    """Pydantic BaseModel with config options for DevCloud usage"""
    class Config:
        """Config options for BaseModel"""
        validate_assignment = True
        allow_population_by_field_name = True
