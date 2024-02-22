"""All pydantic models used in data transformation"""
from typing import Annotated, Literal, Union
from enum import Enum
from abc import abstractmethod
from pydantic import Field, TypeAdapter

from base.model.base_model import ConfiguredBaseModel, S3Path


class StatusProcessing(str, Enum):
    """ Status of the processing pipelines """
    RECEIVED = "received"
    PROCESSING = "processing"
    COMPLETE = "complete"


class PayloadType(str, Enum):
    """ Payload Types """
    SNAPSHOT = "snapshot"
    VIDEO = "video"


class ProcessingStep(str, Enum):
    """ Processing Steps """
    CHC = "CHC"
    ANONYMIZE = "Anonymize"


class ProcessingResult(ConfiguredBaseModel):
    """Generic processing result"""
    correlation_id: str = Field(default=...)

    @property
    @abstractmethod
    def artifact_id(self) -> str:
        """Artifact ID"""


class S3Result(ProcessingResult):
    """A processing result that's based on an S3File"""
    s3_path: S3Path

    @property
    def artifact_id(self) -> str:
        """
        Return file name without extension.
        """
        return self.s3_path.split("/")[-1].split(".")[0]


class SignalsProcessingResult(S3Result):
    """Signals MDF Processed Artifact"""
    artifact_name: Literal["signals_processed"] = "signals_processed"
    tenant_id: str = Field(default=...)
    video_raw_s3_path: S3Path = Field(default=...)
    recording_overview: dict[str, int | float | str] = Field(default=...)


class IMUProcessingResult(S3Result):
    """IMU MDF Processed Result"""
    artifact_name: Literal["imu_processed"] = "imu_processed"
    tenant_id: str = Field(default=...)
    video_raw_s3_path: S3Path = Field(default=...)


class PipelineProcessingStatus(S3Result):
    """Pipelines processing status"""
    artifact_name: Literal["sdm"] = "sdm"
    info_source: str = Field(default=...)
    tenant_id: str = Field(default=...)
    object_type: PayloadType = Field(default=...)
    processing_status: StatusProcessing = Field(default=...)
    processing_steps: list[ProcessingStep] = Field(default=...)


class AnonymizationResult(S3Result):
    """Anonymization result"""
    artifact_name: Literal["anonymize"] = "anonymize"
    raw_s3_path: S3Path
    tenant_id: str = Field(default=...)
    processing_status: StatusProcessing = Field(default=...)


class CHCResult(S3Result):
    """CHC result"""
    artifact_name: Literal["chc"] = "chc"
    tenant_id: str = Field(default=...)
    raw_s3_path: S3Path
    processing_status: StatusProcessing = Field(default=...)


ProcessingResults = Union[SignalsProcessingResult,
                          IMUProcessingResult,
                          AnonymizationResult,
                          CHCResult,
                          PipelineProcessingStatus]

DiscriminatedProcessingResultsTypeAdapter = TypeAdapter(Annotated[ProcessingResults,
                                                                  Field(...,
                                                                        discriminator="artifact_name")])

# autopep8: off
def parse_results(json_data: Union[str, dict]) -> ProcessingResult:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return DiscriminatedProcessingResultsTypeAdapter.validate_python(json_data) # type: ignore
    return DiscriminatedProcessingResultsTypeAdapter.validate_json(json_data) # type: ignore
# autopep8: on
