from mongoengine import Document, StringField, DateTimeField, EnumField, URLField, ReferenceField, EmbeddedDocument, EmbeddedDocumentField, CASCADE
from labeling_bridge.models.database.enums import StatusDocument, DataDeletionStatus, KognicLabelingType


class DataDeletion(EmbeddedDocument):
    data_deletion_status = EnumField(DataDeletionStatus, default=DataDeletionStatus.NOT_REQUESTED)
    deletion_requested_at = DateTimeField(default=None)


class LabelingJobTask(Document):
    labeling_job = ReferenceField("LabelingJob", reverse_delete_rule=CASCADE, required=True)
    exported_at = DateTimeField(default=None)
    media_filepath = StringField(required=True)
    raw_media_filepath = StringField(required=True)
    data_deletion = EmbeddedDocumentField(DataDeletion, default=DataDeletion())
    import_export_status = EmbeddedDocumentField(StatusDocument, default=StatusDocument())
    kognic_labeling_type = EnumField(KognicLabelingType, required=True)
    kognic_task_id = StringField(max_length=200)
