from enum import Enum
from mongoengine import StringField, EnumField, EmbeddedDocument


class Status(Enum):
    NEW = "new"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"


class DataDeletionStatus(Enum):
    NOT_REQUESTED = "not-requested"
    REQUESTED = "requested"
    DATA_DELETED = "data-deleted"


class StatusDocument(EmbeddedDocument):
    status = EnumField(Status, default=Status.NEW)
    message = StringField(default=None)


class KognicLabelingType(Enum):
    SEMSEG = "semseg"
    BODYPOSE = "bodypose"
