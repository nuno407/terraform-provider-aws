""" Upload Rule Model """
from typing import Literal, Annotated, Union
from enum import Enum
from pydantic import Field, TypeAdapter
from base.model.base_model import ConfiguredBaseModel, RawS3Path
from base.model.validators import UtcDatetimeInPast

DEFAULT_RULE_NAME = "automatic"
DEFAULT_RULE_VERSION = "1.0"


class RuleOrigin(str, Enum):
    """ Rule origin """
    SAV = "REMOTE_ASSIST_UI"
    INTERIOR = "MANUAL_UPLOAD"
    PREVIEW = "PREVIEW_RULE_BASED"
    UKNOWN = "UKNOWN"


class SelectorRule(ConfiguredBaseModel):
    """ Represents an upload Rule issued by Selector """
    rule_name: str
    rule_version: str
    origin: RuleOrigin


class BaseUploadRule(ConfiguredBaseModel):
    """ A base class for an upload rule """
    tenant: str
    raw_file_path: RawS3Path
    rule: SelectorRule


class VideoUploadRule(BaseUploadRule):
    """
    Represents a Video Upload Rule that is issue by the Selector to the footage API.
    """
    artifact_name: Literal["video_rule"] = "video_rule"
    video_id: str
    footage_from: UtcDatetimeInPast
    footage_to: UtcDatetimeInPast


class SnapshotUploadRule(BaseUploadRule):
    """
    Represents a Snapshot Upload Rule that is issue by the Selector to the footage API.
    As of now, snapshots are automatically uploaded by ridecare.
    """
    artifact_name: Literal["snapshot_rule"] = "snapshot_rule"
    snapshot_id: str
    snapshot_timestamp: UtcDatetimeInPast


# RuleType used to specify when a video does not have any rule to be matched
DEFAULT_RULE = SelectorRule(rule_name=DEFAULT_RULE_NAME, rule_version=DEFAULT_RULE_VERSION, origin=RuleOrigin.UKNOWN)

UploadRules = Union[SnapshotUploadRule, VideoUploadRule]
DiscriminatedUploadRulesTypeAdapter = TypeAdapter(Annotated[UploadRules,
                                                            Field(...,
                                                                  discriminator="artifact_name")])


def parse_upload_rule(json_data: Union[str, dict]) -> ConfiguredBaseModel:
    """Parse artifact from string"""
    if isinstance(json_data, dict):
        return DiscriminatedUploadRulesTypeAdapter.validate_python(json_data)  # type: ignore
    return DiscriminatedUploadRulesTypeAdapter.validate_json(json_data)  # type: ignore
