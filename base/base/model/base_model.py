""" pydantic model config """
from typing import Annotated
from pydantic import ConfigDict, BaseModel, Field

S3Path = Annotated[str, Field(pattern="s3://.+/.+", default=...)]
AnonymizedS3Path = Annotated[str, Field(pattern="s3://.*anonymized.*/.+", default=...)]
RawS3Path = Annotated[str, Field(pattern="s3://.*raw.*/.+", default=...)]


class ConfiguredBaseModel(BaseModel):
    """Pydantic BaseModel with config options for DevCloud usage"""
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, arbitrary_types_allowed=True)

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return self.model_dump_json(by_alias=True, exclude_unset=False, exclude_none=True)
