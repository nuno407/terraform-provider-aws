""" Voxel Rule Model """
from datetime import datetime
from base.model.artifacts import ConfiguredBaseModel


class UploadVideoRuleEmbeddedDocument(ConfiguredBaseModel):
    """ Upload video rule. """
    name: str
    version: str
    origin: str
    footage_from: datetime
    footage_to: datetime


class UploadSnapshotRuleEmbeddedDocument(ConfiguredBaseModel):
    """ Upload video rule. """
    name: str
    version: str
    origin: str
    snapshot_timestamp: datetime
