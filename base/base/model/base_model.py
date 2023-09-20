""" pydantic model config """
from typing import Annotated

from pydantic import BaseModel, Field
from pydantic.generics import GenericModel

S3Path = Annotated[str, Field(regex="s3://.+/.+", default=...)]

class ConfiguredBaseModel(BaseModel):
    """Pydantic BaseModel with config options for DevCloud usage"""
    class Config:
        """Config options for BaseModel"""
        validate_assignment = True
        allow_population_by_field_name = True


class ConfiguredGenericModel(GenericModel):
    """Pydantic GenericModel with config options for DevCloud usage"""
    class Config:
        """Config options for GenericModel"""
        validate_assignment = True
        allow_population_by_field_name = True
