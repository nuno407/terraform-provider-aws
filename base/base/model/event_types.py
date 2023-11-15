"""This module contains the data models for the event artifacts"""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import Field

from base.model.base_model import ConfiguredBaseModel


class EventType(str, Enum):
    """ event type enumerator """
    INCIDENT = "com.bosch.ivs.incident.IncidentEvent"
    DEVICE_INFO = "com.bosch.ivs.DeviceInfoEvent"
    CAMERA_SERVICE = "com.bosch.ivs.camera.CameraServiceEvent"


class LocationStatus(str, Enum):
    """ location status enumerator """
    UNKNOWN = "LOCATION_DATA_STATUS__UNKNOWN"
    VALID = "LOCATION_DATA_STATUS__FIX"
    NO_FIX = "LOCATION_DATA_STATUS__NO_FIX"
    LAST_KNOWN = "LOCATION_DATA_STATUS__LAST_KNOWN_POSITION"
    FEATURE_NOT_AVAILABLE = "LOCATION_DATA_STATUS__FEATURE_NOT_AVAILABLE"
    FEATURE_FAILED = "LOCATION_DATA_STATUS__FEATURE_FAILED"


class Speed(ConfiguredBaseModel):
    """Represents a speed from RCC Location"""
    speed: Optional[float] = Field(default=None)
    speed_accuracy: Optional[float] = Field(default=None)


class Heading(ConfiguredBaseModel):
    """Represents a heading from RCC Location"""
    heading: Optional[float] = Field(default=None)
    heading_accuracy: Optional[float] = Field(default=None)


class Location(ConfiguredBaseModel):
    """Represents a location from RCC"""
    status: LocationStatus = Field(default=LocationStatus.UNKNOWN)
    latitude: Optional[float] = Field(default=None)
    longitude: Optional[float] = Field(default=None)
    location_accuracy: Optional[float] = Field(default=None)
    heading: Heading = Field(default_factory=Heading)
    horizontal_speed: Speed = Field(default_factory=Speed)


class IncidentType(str, Enum):
    """ incident type enumerator, used for incident events """
    UNKNOWN = "INCIDENT_TYPE__UNKNOWN"
    PANIC = "INCIDENT_TYPE__PANIC"
    ACCIDENT_MANUAL = "INCIDENT_TYPE__ACCIDENT_MANUAL"
    PANIC_CANCEL = "INCIDENT_TYPE__PANIC_CANCEL"
    ACCIDENT_AUTO = "INCIDENT_TYPE__ACCIDENT_AUTOMATIC"


class ShutdownReason(str, Enum):
    """ shutdown reason enumerator, used for device info events """
    UNKNOWN = "SHUTDOWN_REASON__UNKNOWN"
    USER_INITIATED = "SHUTDOWN_REASON__USER_INITIATED"
    BATTERY_LOW = "SHUTDOWN_REASON__BATTERY_LOW"
    BATTERY_DISCHARCHING_TIMEOUT = "SHUTDOWN_REASON__BATTERY_DISCHARCHING_TIMEOUT"
    POWER_PRESENT_TIMEOUT = "SHUTDOWN_REASON__POWER_PRESENT_TIMEOUT"
    TEMPERATURE_WARNING = "SHUTDOWN_REASON__TEMPERATURE_WARNING"
    SYSTEM_COMMAND = "SHUTDOWN_REASON__SYSTEM_COMMAND"
    INACTIVITY = "SHUTDOWN_REASON__INACTIVITY"
    POSSIBLE_CRASH = "SHUTDOWN_REASON__POSSIBLE_CRASH"
    POWER_SUPPLY_LOW = "SHUTDOWN_REASON__POWER_SUPPLY_LOW"
    REGULAR_REBOOT = "SHUTDOWN_REASON__REGULAR_REBOOT"
    EMERGENCY_REBOOT = "SHUTDOWN_REASON__EMERGENCY_REBOOT"


class Shutdown(ConfiguredBaseModel):
    """Details about the last shutdown"""
    reason: ShutdownReason = Field(default=ShutdownReason.UNKNOWN, alias="shutdown_reason")
    reason_description: Optional[str] = Field(default=None, alias="shutdown_reason_description")
    timestamp: Optional[datetime] = Field(default=None, alias="timestamp_ms")


class GeneralServiceState(str, Enum):
    """service state enumerator, used for camera service events"""
    UNKNOWN = "SERVICE_STATUS__UNKNOWN"
    AVAILABLE = "SERVICE_STATUS__AVAILABLE"
    RESTRICTED_AVAILABLE = "SERVICE_STATUS__RESTRICTED_AVAILABLE"
    INACTIVE = "SERVICE_STATUS__INACTIVE"
    ERROR = "SERVICE_STATUS__ERROR"
    NOT_INITIALIZED = "SERVICE_STATUS__NOT_INITIALIZED"
    INACTIVE_NOT_REQUESTABLE = "SERVICE_STATUS__INACTIVE_NOT_REQUESTABLE"


class CameraServiceState(str, Enum):
    """camera service state enumerator, used for camera service events"""
    UNKNOWN = "CAMERA_SERVICE_DESCRIPTION__UNKNOWN"
    CAMERA_READY = "CAMERA_SERVICE_DESCRIPTION__CAMERA_READY"
    CAMERA_BLOCKED = "CAMERA_SERVICE_DESCRIPTION__CAMERA_BLOCKED"
    CAMERA_NO_SIGNAL_PROCESSING = "CAMERA_SERVICE_DESCRIPTION__CAMERA_NO_SIGNAL_PROCESSING"
    CAMERA_NO_VIDEO_SIGNAL = "CAMERA_SERVICE_DESCRIPTION__CAMERA_NO_VIDEO_SIGNAL"
    CAMERA_NOT_ALIVE = "CAMERA_SERVICE_DESCRIPTION__CAMERA_NOT_ALIVE"
    CAMERA_SHIFTED = "CAMERA_SERVICE_DESCRIPTION__CAMERA_SHIFTED"
