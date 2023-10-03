"""All pydantic models"""
from base.model.artifacts.artifacts import *
from base.model.artifacts.processing_result import *

DiscriminatedArtifacts = Annotated[Union[ProcessingResults, Artifacts],Field(...,discriminator="artifact_name")]
