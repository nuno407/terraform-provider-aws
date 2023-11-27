""" Upload Rule Model """
from base.model.base_model import ConfiguredBaseModel
from base.model.validators import UtcDatetimeInPast


class VideoUploadRule(ConfiguredBaseModel):
    """
    Represents a Video Upload Rule that is issue by the Selector to the footage API.
    """
    video_id: str
    rule_name: str
    rule_version: str
    footage_from: UtcDatetimeInPast
    footage_to: UtcDatetimeInPast


class SnapshotUploadRule(ConfiguredBaseModel):
    """
    Represents a Snapshot Upload Rule that is issue by the Selector to the footage API.
    As of now, snapshots are automatically uploaded by ridecare.
    """
    snapshot_id: str
    rule_name: str
    rule_version: str
    snapshot_timestamp: UtcDatetimeInPast
