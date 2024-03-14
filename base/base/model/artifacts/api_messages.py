"""Metadata API messages"""
from typing import Union, Literal, Annotated
from pydantic import RootModel, TypeAdapter, Field, StrictBool
import pandas as pd
import pandera as pa
from base.model.validators import LegacyTimeDelta
from base.model.artifacts.artifacts import SignalsArtifact
from base.model.artifacts.processing_result import IMUProcessingResult, CHCResult
from base.model.base_model import ConfiguredBaseModel, S3Path
from base.model.metadata.media_metadata import MediaMetadata


class IMUSource(ConfiguredBaseModel):
    """IMU Source"""
    device_id: str
    tenant: str


class IMUProcessedData(pa.DataFrameModel):
    """IMUProcessedData"""
    source: pa.typing.Series[pa.Object]  # Validation with TypedDict is 460x slower than with an object
    timestamp: pa.typing.Series[pd.DatetimeTZDtype] = pa.Field(dtype_kwargs={"tz": "UTC"})
    gyr_y_mean: pa.typing.Series[pa.Float]
    gyr_x_var: pa.typing.Series[pa.Float]
    gyr_z_max: pa.typing.Series[pa.Float]
    acc_z_var: pa.typing.Series[pa.Float]
    acc_z_max: pa.typing.Series[pa.Float]
    gyr_y_var: pa.typing.Series[pa.Float]
    acc_y_min: pa.typing.Series[pa.Float]
    gyr_z_var: pa.typing.Series[pa.Float]
    acc_z_mean: pa.typing.Series[pa.Float]
    acc_x_max: pa.typing.Series[pa.Float]
    acc_y_mean: pa.typing.Series[pa.Float]
    gyr_x_mean: pa.typing.Series[pa.Float]
    acc_y_max: pa.typing.Series[pa.Float]
    gyr_y_min: pa.typing.Series[pa.Float]
    acc_y_var: pa.typing.Series[pa.Float]
    acc_z_min: pa.typing.Series[pa.Float]
    acc_x_mean: pa.typing.Series[pa.Float]
    gyr_x_max: pa.typing.Series[pa.Float]
    acc_x_min: pa.typing.Series[pa.Float]
    gyr_z_mean: pa.typing.Series[pa.Float]
    gyr_x_min: pa.typing.Series[pa.Float]
    gyr_y_max: pa.typing.Series[pa.Float]
    acc_x_var: pa.typing.Series[pa.Float]
    gyr_z_min: pa.typing.Series[pa.Float]

    class Config:  # pylint: disable=too-few-public-methods
        """Config for pandera model"""
        coerce = True
        from_format = "parquet"


class SignalsFrame(RootModel):
    """SignalsFrame"""
    root: dict[str, Union[StrictBool, int, float]]


class VideoSignalsData(ConfiguredBaseModel):
    """Video Signals file"""
    artifact_name: Literal["video_signals_data"] = "video_signals_data"
    data: dict[LegacyTimeDelta, SignalsFrame]
    aggregated_metadata: dict[str, StrictBool | int | float | str]
    correlation_id: str
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
    data: str  # This a base 64 encoded parquet file ->  containing this "IMUProcessedData"


class CHCDataResult(ConfiguredBaseModel):
    """CHC Data result"""
    artifact_name: Literal["chc_data_result"] = "chc_data_result"
    message: CHCResult
    data: dict[LegacyTimeDelta, SignalsFrame]


APIMessages = Union[VideoSignalsData,
                    SnapshotSignalsData, IMUDataArtifact, CHCDataResult]

DiscriminatedAPIMessagesTypeAdapter = TypeAdapter(Annotated[APIMessages,
                                                  Field(..., discriminator="artifact_name")])
