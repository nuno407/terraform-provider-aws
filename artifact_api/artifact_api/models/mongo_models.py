"""Mongo Models Module"""

from datetime import datetime
from typing import Optional, Literal
from pydantic import Field
from base.model.base_model import ConfiguredBaseModel
from base.model.event_types import (CameraServiceState,
                                    GeneralServiceState, IncidentType,
                                    Location, ShutdownReason)
from base.model.validators import UtcDatetimeInPast


class DBVideoRecordingOverview(ConfiguredBaseModel):
    """Video Recording Overview in the format used in the database"""

    snapshots: int = Field(alias="#snapshots")
    devcloud_id: str = Field(alias="devcloudid")
    device_id: str = Field(alias="deviceID")
    length: str = Field(pattern=r"^\d?:\d{2}:\d{2}$")
    recording_duration: float
    recording_time: UtcDatetimeInPast = Field(default=...)
    snapshots_paths: list[str] = []
    tenant_id: str = Field(alias="tenantID")
    time: str = Field(pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    chc_duration: Optional[float] = Field(default=None)
    gnss_coverage: Optional[float] = Field(default=None)
    max_audio_loudness: Optional[float] = Field(default=None)
    max_person_count: Optional[int] = Field(default=None)
    mean_audio_bias: Optional[float] = Field(default=None)
    median_person_count: Optional[int] = Field(default=None)
    number_chc_events: Optional[int] = Field(default=None)
    ride_detection_people_count_after: Optional[int] = Field(default=None)
    ride_detection_people_count_before: Optional[int] = Field(default=None)
    sum_door_closed: Optional[int] = Field(default=None)
    variance_person_count: Optional[float] = Field(default=None)


class DBSnapshotRecordingOverview(ConfiguredBaseModel):
    """Snapshot Recording Overview in the format used in the database"""
    devcloud_id: str = Field(alias="devcloudid")
    device_id: str = Field(alias="deviceID")
    tenant_id: str = Field(alias="tenantID")
    recording_time: UtcDatetimeInPast
    source_videos: list[str] = []


class DBCameraServiceEventArtifact(ConfiguredBaseModel):
    """Represents a camera service event from RCC"""

    tenant_id: str
    device_id: str
    timestamp: UtcDatetimeInPast = Field(default=...)
    event_name: str = Field(default=...)
    artifact_name: Literal["camera_service"] = "camera_service"
    service_state: GeneralServiceState = Field(default=GeneralServiceState.UNKNOWN)
    camera_name: Optional[str] = Field(default=None)
    camera_state: list[CameraServiceState] = Field(default_factory=list)


class DBShutdown(ConfiguredBaseModel):
    """Details about the last shutdown"""
    reason: ShutdownReason = Field(default=ShutdownReason.UNKNOWN)
    reason_description: Optional[str] = Field(default=None)
    timestamp: Optional[datetime] = Field(default=None)


class DBDeviceInfoEventArtifact(ConfiguredBaseModel):
    """Represents a device info event from RCC"""
    tenant_id: str
    device_id: str
    timestamp: UtcDatetimeInPast = Field(default=...)
    artifact_name: Literal["device_info"] = "device_info"
    event_name: str = Field(default=...)
    system_report: Optional[str] = Field(default=None)
    software_versions: list[dict[str, str]] = Field(default_factory=list)
    device_type: Optional[str] = Field(default=None)
    last_shutdown: Optional[DBShutdown] = Field(default=None)


class DBIncidentEventArtifact(ConfiguredBaseModel):
    """Represents a incident event from RCC"""
    tenant_id: str
    device_id: str
    timestamp: UtcDatetimeInPast = Field(default=...)
    artifact_name: Literal["incident_info"] = "incident_info"
    event_name: str = Field(default=...)
    location: Optional[Location] = Field(default=None)
    incident_type: IncidentType = Field(default=IncidentType.UNKNOWN)
    bundle_id: Optional[str] = Field(default=None)


class DBSnapshotArtifact(ConfiguredBaseModel):
    """Snapshot Artifact in the format used in the database"""
    video_id: str
    media_type: Literal["image"] = Field(default="image", alias="_media_type")
    filepath: str
    recording_overview: DBSnapshotRecordingOverview


class DBS3VideoArtifact(ConfiguredBaseModel):
    """S3Video Artifact in the format used in the database"""
    video_id: str
    mdf_available: str = Field(default="No", alias="MDF_available")
    media_type: Literal["video"] = Field(default="video", alias="_media_type")
    filepath: str
    resolution: str = Field(pattern=r"\d+x\d+")
    recording_overview: DBVideoRecordingOverview


class DBIMUSource(ConfiguredBaseModel):
    """DBIMUSource used in the IMUSample model"""
    device_id: str
    tenant: str


class DBIMUSample(ConfiguredBaseModel):
    """IMU Sample in the format used in the database"""
    source: DBIMUSource
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
