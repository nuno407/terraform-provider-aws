"""Metadata API messages"""
from typing import Union
from pydantic import RootModel
from base.model.validators import LegacyTimeDelta, UtcDatetimeInPast
from base.model.artifacts import IMUArtifact, SignalsArtifact
from base.model.base_model import ConfiguredBaseModel


class IMUSource(ConfiguredBaseModel):
    """IMU Source"""
    device_id: str
    tenant: str


class IMUSample(ConfiguredBaseModel):
    "IMU Sample"
    source: IMUSource
    timestamp: UtcDatetimeInPast  # This might cause slow parsing, needs investigation on large files
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


class VideoSignalsData(RootModel):
    """Video Signals file"""
    root: dict[LegacyTimeDelta, SignalsFrame]


class SnapshotSignalsData(ConfiguredBaseModel):
    """Snapshot signals data"""
    message: SignalsArtifact
    data: VideoSignalsData


class IMUDataArtifact(ConfiguredBaseModel):
    """IMUData"""
    message: IMUArtifact
    data: IMUProcessedData


class CHCDataResult(ConfiguredBaseModel):
    """CHC Data result"""
    id: str
    chc_path: str
    data: VideoSignalsData
