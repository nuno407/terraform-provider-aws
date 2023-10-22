"""All pydantic models"""
from base.model.artifacts.artifacts import *
from base.model.artifacts.processing_result import *

AnnotatedArtifacts = Annotated[Union[ProcessingResults, RCCArtifacts], Field(..., discriminator="artifact_name")]
DiscriminatedArtifactsTypeAdapter = TypeAdapter(AnnotatedArtifacts)
