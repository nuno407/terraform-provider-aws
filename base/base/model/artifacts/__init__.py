"""All pydantic models"""
from typing import Union
from base.model.artifacts.api_messages import *
from base.model.artifacts.artifacts import *
from base.model.artifacts.processing_result import *
from base.model.artifacts.upload_rule_model import *
from base.model.base_model import ConfiguredBaseModel

AnnotatedArtifacts = Annotated[Union[ProcessingResults, RCCArtifacts,
                                     UploadRules, APIMessages], Field(..., discriminator="artifact_name")]
DiscriminatedArtifactsTypeAdapter = TypeAdapter(AnnotatedArtifacts)


def parse_all_models(data: Union[dict, str]) -> ConfiguredBaseModel:
    """Parse models from string or dict"""
    if isinstance(data, dict):
        return DiscriminatedArtifactsTypeAdapter.validate_python(data)  # type: ignore
    return DiscriminatedArtifactsTypeAdapter.validate_json(data)  # type: ignore
