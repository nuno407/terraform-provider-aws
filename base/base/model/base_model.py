""" pydantic model config """
from typing import Annotated
from pydantic import ConfigDict, BaseModel, Field

S3Path = Annotated[str, Field(pattern="s3://.+/.+", default=...)]


class ConfiguredBaseModel(BaseModel):
    """Pydantic BaseModel with config options for DevCloud usage"""
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)
