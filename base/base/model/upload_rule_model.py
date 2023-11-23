""" Upload Rule Model """
from base.model.base_model import ConfiguredBaseModel, RawS3Path
from base.model.validators import UtcDatetimeInPast


class UploadRule(ConfiguredBaseModel):
    """
    Represents a Upload Rule that is issue by the Selector to the footage API.
    Only upload of videos are considered.
    """
    s3_path: RawS3Path
    rule_name: str
    rule_version: str
    footage_from: UtcDatetimeInPast
    footage_to: UtcDatetimeInPast
