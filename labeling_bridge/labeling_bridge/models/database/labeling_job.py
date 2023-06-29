""" Database model """
import datetime
from mongoengine import Document, StringField, DateTimeField, EmailField, DictField, EmbeddedDocumentField

from labeling_bridge.models.database.enums import StatusDocument


class LabelingJob(Document):
    """ LabelingJob """
    created_by = EmailField(required=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow, required=True)
    voxel_dataset = StringField(max_length=200, required=True)
    voxel_query_dump = DictField()
    kognic_project_id = StringField(max_length=200, required=True)
    kognic_labeling_job_name = StringField(max_length=200, required=True)
    import_export_status = EmbeddedDocumentField(StatusDocument, default=StatusDocument(), required=True)

    meta = {"db_alias": "DataPrivacyDB", "collection": "labeling-job"}
