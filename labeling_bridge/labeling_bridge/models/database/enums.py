""" Database models. """
from enum import Enum
from mongoengine import StringField, EnumField, EmbeddedDocument


class Status(Enum):
    """ Status """
    NEW = "new"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"


class DataDeletionStatus(Enum):
    """ DataDeletionStatus """
    NOT_REQUESTED = "not-requested"
    REQUESTED = "requested"
    DATA_DELETED = "data-deleted"


class StatusDocument(EmbeddedDocument):
    """ StatusDocument """
    status = EnumField(Status, default=Status.NEW)
    message = StringField(default=None)


class KognicLabelingType(Enum):
    """ KognicLabelingType """
    SEMSEG = "semseg"
    BODYPOSE = "bodypose"
