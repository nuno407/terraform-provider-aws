"""" Database models """
from mongoengine import Document, StringField, DateTimeField, EnumField, \
    ReferenceField, EmbeddedDocument, EmbeddedDocumentField, CASCADE
from labeling_bridge.models.database.enums import StatusDocument, \
    DataDeletionStatus, KognicLabelingType


class DataDeletion(EmbeddedDocument):
    """ DataDeletion """
    status = EnumField(DataDeletionStatus, default=DataDeletionStatus.NOT_REQUESTED)
    deletion_requested_at = DateTimeField(default=None)


class LabelingJobTask(Document):
    """ LabelingJobTask """
    kognic_labeling_job = ReferenceField("LabelingJob", reverse_delete_rule=CASCADE)
    exported_to_kognic_at = DateTimeField(default=None)
    media_filepath = StringField(required=True)
    raw_media_filepath = StringField(required=True)
    data_deletion = EmbeddedDocumentField(DataDeletion, default=DataDeletion())
    import_export_status = EmbeddedDocumentField(StatusDocument, default=StatusDocument())
    kognic_labeling_type = EnumField(KognicLabelingType, required=True)
    kognic_task_id = StringField(max_length=200)

    meta = {"db_alias": "DataPrivacyDB"}
