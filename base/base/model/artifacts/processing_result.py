"""All pydantic models used in data transformation"""
from typing import Annotated, Literal, Union
from enum import Enum
from abc import abstractmethod
from pydantic import Field, parse_obj_as, parse_raw_as
from base.model.base_model import ConfiguredBaseModel, S3Path


class StatusProcessing(str, Enum):
    """ Status of the processing pipelines """
    RECEIVED = "received"
    PROCESSING = "processing"
    COMPLETE = "complete"


class ProcessingStep(str, Enum):
    """ Processing Steps """
    CHC = "CHC"
    ANONYMIZE = "Anonymize"


class ProcessingResult(ConfiguredBaseModel):
    """Generic processing result"""
    correlation_id: str = Field(default=...)

    def stringify(self) -> str:
        """ stringifies the artifact. """
        return self.json(by_alias=True, exclude_unset=False, exclude_none=True)

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
    recording_overview: dict[str, Union[float, int, str]] = Field(default=...)


class IMUProcessingResult(S3Result):
    """IMU MDF Processed Result"""
    artifact_name: Literal["imu_processed"] = "imu_processed"


class PipelineProcessingStatus(S3Result):
    """Pipelines processing status"""
    artifact_name: Literal["sdm"] = "sdm"
    processing_status: StatusProcessing = Field(default=...)
    processing_steps: list[ProcessingStep] = Field(default=...)


class AnonymizationResult(S3Result):
    """Anonymization result"""
    artifact_name: Literal["anonymize"] = "anonymize"
    raw_s3_path: S3Path
    processing_status: StatusProcessing = Field(default=...)


class CHCResult(S3Result):
    """CHC result"""
    artifact_name: Literal["chc"] = "chc"
    raw_s3_path: S3Path
    processing_status: StatusProcessing = Field(default=...)


DiscriminatedProcessingResult = Annotated[Union[SignalsProcessingResult,      # pylint: disable=invalid-name
                                                IMUProcessingResult,
                                                AnonymizationResult,
                                                CHCResult,
                                                PipelineProcessingStatus],
                                          Field(...,
                                                discriminator="artifact_name")]


def parse_results(json_data: Union[str, dict]) -> ProcessingResult:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return parse_obj_as(DiscriminatedProcessingResult,  # type: ignore
                            json_data)
    return parse_raw_as(DiscriminatedProcessingResult,  # type: ignore
                        json_data)
