"""Metadata API messages"""
from typing import Union, Literal, Annotated
from pydantic import RootModel, TypeAdapter, Field

from base.model.validators import LegacyTimeDelta
from base.model.artifacts.artifacts import SignalsArtifact
from base.model.artifacts.processing_result import IMUProcessingResult
from base.model.base_model import ConfiguredBaseModel, S3Path
from base.model.metadata.media_metadata import MediaMetadata


class IMUSource(ConfiguredBaseModel):
    """IMU Source"""
    device_id: str
    tenant: str


class IMUSample(ConfiguredBaseModel):
    """IMUSample"""
    source: IMUSource
    timestamp: int  # This might cause slow parsing, needs investigation on large files
    gyr_y_mean: float
    gyr_x_var: float
    gyr_z_max: float
    acc_z_var: float
    acc_z_max: float
    gyr_y_var: float
    acc_y_min: float
    gyr_z_var: float
    acc_z_mean: float
    acc_x_max: float
    acc_y_mean: float
    gyr_x_mean: float
    acc_y_max: float
    gyr_y_min: float
    acc_y_var: float
    acc_z_min: float
    acc_x_mean: float
    gyr_x_max: float
    acc_x_min: float
    gyr_z_mean: float
    gyr_x_min: float
    gyr_y_max: float
    acc_x_var: float
    gyr_z_min: float


class IMUProcessedData(RootModel):
    """IMUProcessedData"""
    root: list[IMUSample]


class SignalsFrame(RootModel):
    """SignalsFrame"""
    root: dict[str, Union[int, float, bool]]


class VideoSignalsData(ConfiguredBaseModel):
    """Video Signals file"""
    artifact_name: Literal["video_signals_data"] = "video_signals_data"
    data: dict[LegacyTimeDelta, SignalsFrame]
    agregated_metadata: dict[str, Union[str,int,float,bool]]
    correlation_id : str
    tenant_id: str
    video_raw_s3_path: S3Path


class SnapshotSignalsData(ConfiguredBaseModel):
    """Snapshot signals data"""
    artifact_name: Literal["snapshot_signals_data"] = "snapshot_signals_data"
    message: SignalsArtifact
    data: MediaMetadata


class IMUDataArtifact(ConfiguredBaseModel):
    """IMUData"""
    artifact_name: Literal["imu_data_artifact"] = "imu_data_artifact"
    message: IMUProcessingResult
    data: IMUProcessedData


class CHCDataResult(ConfiguredBaseModel):
    """CHC Data result"""
    artifact_name: Literal["chc_data_result"] = "chc_data_result"
    id: str
    chc_path: str
    data: VideoSignalsData


APIMessages = Union[VideoSignalsData, SnapshotSignalsData, IMUDataArtifact, CHCDataResult]

DiscriminatedAPIMessagesTypeAdapter = TypeAdapter(Annotated[APIMessages,
                                                  Field(..., discriminator="artifact_name")])
