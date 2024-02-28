"""Mongo Models Module"""
from enum import Enum
from datetime import datetime, timedelta
from typing import Any, Optional, Literal
from typing_extensions import Annotated
from pydantic import Field, model_serializer, SerializationInfo
from pydantic.functional_serializers import PlainSerializer
from base.model.artifacts.processing_result import StatusProcessing, ProcessingStep
from base.model.base_model import ConfiguredBaseModel
from base.model.event_types import (CameraServiceState, GeneralServiceState,
                                    IncidentType, ShutdownReason)
from base.model.artifacts.upload_rule_model import RuleOrigin
from base.model.validators import UtcDatetimeInPast


def serialize_signals_dict(signals: dict[timedelta, dict[str, int | float | bool]]  # pylint: disable=no-self-argument
                           ) -> dict[str, dict[str, int | float | bool]]:
    """Serialize the signals dict"""
    return {
        "{:01d}:{:02d}:{:02d}.{:06d}".format(  # pylint: disable=consider-using-f-string
            td.seconds // 3600, (td.seconds // 60) %
            60, td.seconds %
            60, td.microseconds): val for td, val in signals.items()}


SignalsSerializer = Annotated[dict[timedelta, dict[str, int | float | bool]],
                              PlainSerializer(serialize_signals_dict)]


class DBVideoUploadRule(ConfiguredBaseModel):
    """
    Represents a DB Video Upload Rule.
    """
    name: str
    version: str
    footage_from: UtcDatetimeInPast
    footage_to: UtcDatetimeInPast
    origin: RuleOrigin


class DBSnapshotUploadRule(ConfiguredBaseModel):
    """
    Represents a DB Snapshot Upload Rule.
    """
    name: str
    version: str
    snapshot_timestamp: UtcDatetimeInPast
    origin: RuleOrigin


class DBVideoRecordingOverview(ConfiguredBaseModel):
    """Video Recording Overview in the format used in the database"""

    snapshots: Optional[int] = Field(alias="#snapshots", default=None)
    devcloud_id: Optional[str] = Field(alias="devcloudid", default=None)
    device_id: Optional[str] = Field(alias="deviceID", default=None)
    length: Optional[str] = Field(pattern=r"^\d?:\d{2}:\d{2}$", default=None)
    recording_duration: Optional[float] = Field(default=None)
    recording_time: Optional[UtcDatetimeInPast] = Field(default=None)
    snapshots_paths: Optional[list[str]] = Field(default=None)
    tenant_id: Optional[str] = Field(alias="tenantID", default=None)
    time: Optional[str] = Field(
        pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", default=None)
    aggregated_metadata: Optional[dict[str, bool |
                                       int | float | str]] = Field(default=None)

    @model_serializer(mode="wrap")
    def serialize_aggregated_metadata(self, serializer: Any, _: SerializationInfo) -> Any:
        """
        Serializes the recording overview and flattens the aggregated_metadata to comply with
        the current DB schema.

        Based on: https://github.com/pydantic/pydantic/issues/6575
        """
        partial_result = serializer(self)
        if "aggregated_metadata" not in partial_result:
            return partial_result

        if partial_result["aggregated_metadata"] is None:
            del partial_result["aggregated_metadata"]
            return partial_result

        for m_key, m_value in partial_result["aggregated_metadata"].items():
            partial_result[m_key] = m_value
        del partial_result["aggregated_metadata"]
        return partial_result


class DBSnapshotRecordingOverview(ConfiguredBaseModel):
    """Snapshot Recording Overview in the format used in the database"""
    devcloud_id: str = Field(alias="devcloudid")
    device_id: str = Field(alias="deviceID")
    tenant_id: str = Field(alias="tenantID")
    recording_time: UtcDatetimeInPast
    source_videos: Optional[list[str]] = Field(default=None)


class DBCameraServiceEventArtifact(ConfiguredBaseModel):
    """Represents a camera service event from RCC"""

    tenant_id: str
    device_id: str
    timestamp: UtcDatetimeInPast = Field(default=...)
    event_name: str = Field(default=...)
    artifact_name: Literal["camera_service"] = "camera_service"
    service_state: GeneralServiceState = Field(
        default=GeneralServiceState.UNKNOWN)
    camera_name: Optional[str] = Field(default=None)
    camera_state: list[CameraServiceState] = Field(default_factory=list)


class DBShutdown(ConfiguredBaseModel):
    """Details about the last shutdown"""
    reason: ShutdownReason = Field(
        default=ShutdownReason.UNKNOWN)
    reason_description: Optional[str] = Field(
        default=None)
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
    incident_type: IncidentType = Field(default=IncidentType.UNKNOWN)
    bundle_id: Optional[str] = Field(default=None)
    imu_available: Optional[bool] = Field(default=None)


class DBSnapshotArtifact(ConfiguredBaseModel):
    """Snapshot Artifact in the format used in the database"""
    # In our DB, Snapshots can be uniquely identified by video_id and _media_type
    # Making it a composed primary key
    video_id: str
    media_type: Literal["image"] = Field(default="image", alias="_media_type")
    filepath: Optional[str]
    recording_overview: Optional[DBSnapshotRecordingOverview]
    upload_rules: Optional[list[DBSnapshotUploadRule]] = Field(default=None)


class DBS3VideoArtifact(ConfiguredBaseModel):
    """S3Video Artifact in the format used in the database"""
    # In our DB, Snapshots can be uniquely identified by video_id and _media_type
    # Making it a composed primary key
    video_id: str
    media_type: Literal["video"] = Field(default="video", alias="_media_type")
    mdf_available: Optional[str] = Field(default=None, alias="MDF_available")
    filepath: Optional[str] = Field(default=None)
    resolution: Optional[str] = Field(default=None, pattern=r"\d+x\d+")
    recording_overview: Optional[DBVideoRecordingOverview]
    upload_rules: Optional[list[DBVideoUploadRule]] = Field(default=None)


class DBIMUSource(ConfiguredBaseModel):
    """DBIMUSource used in the IMUSample model"""
    device_id: str
    tenant: str


class DBIMUSample(ConfiguredBaseModel):
    """IMU Sample in the format used in the database"""
    source: DBIMUSource
    # This might cause slow parsing, needs investigation on large files
    timestamp: UtcDatetimeInPast
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


class DBPipelineProcessingStatus(ConfiguredBaseModel):
    """Pipeline Processing Status in the format used in the database"""
    _id: str
    s3_path: str
    info_source: str
    last_updated: str
    from_container: Literal["Metadata"] = Field(default="Metadata")
    processing_status: StatusProcessing
    processing_list: list[ProcessingStep] = Field(default=...)


class SignalsSource(str, Enum):
    """Signals source"""
    MDF_PARSER = "MDFParser"
    CHC = "CHC"


class DBSignals(ConfiguredBaseModel):
    """Signals in the format used in the database"""
    recording: str
    signals: SignalsSerializer
    source: SignalsSource
    algo_out_id: Optional[str] = Field(default=None)


class DBOutputPath(ConfiguredBaseModel):
    """Output path"""
    video: Optional[str] = Field(default=None)
    metadata: Optional[str] = Field(default=None)


class DBAnonymizationResult(ConfiguredBaseModel):
    """Anonymization result"""
    _id: str
    algorithm_id: Literal["Anonymize"] = Field(default="Anonymize")
    pipeline_id: str
    output_paths: DBOutputPath


class DBResults(ConfiguredBaseModel):
    """Signals Results"""
    CHBs_sync: SignalsSerializer


class DBCHCResult(ConfiguredBaseModel):
    """CHC result"""
    _id = str
    algorithm_id: Literal["CHC"] = Field(default="CHC")
    pipeline_id: str
    output_paths: DBOutputPath
    results: DBResults
