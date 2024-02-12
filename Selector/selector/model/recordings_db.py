"""Module containing mongoengine models for recordings collection."""
import os
from mongoengine import (DynamicDocument, DynamicEmbeddedDocument, StringField,
                         DateTimeField, ListField, EmbeddedDocumentField, FloatField)

RECORDINGS_COLLECTION = os.environ["MONGO_RECORDINGS_COLLECTION"]


class DBRecordingRule(DynamicEmbeddedDocument):
    """
        Keep in mind that this Mongo Document is not defined by the selector component.
        Artifact API/ Downloader is the one responsible by this schema.
        This is the reason that this is marked as DynamicDocument, to prevent breaking changes
    """
    name = StringField(required=True)
    version = StringField(required=True)
    footage_from = DateTimeField(required=True)
    footage_to = DateTimeField(required=True)


class DBRecordingOverview(DynamicEmbeddedDocument):
    """
        Keep in mind that this Mongo Document is not defined by the selector component.
        Artifact API/ Downloader is the one responsible by this schema.
        This is the reason that this is marked as DynamicDocument, to prevent breaking changes
    """
    deviceID = StringField(required=False)
    tenantID = StringField(required=False)
    recording_duration = FloatField(required=False)
    recording_time = DateTimeField(required=False)


class DBRecording(DynamicDocument):
    """
        Keep in mind that this Mongo Document is not defined by the selector component.
        Artifact API/ Downloader is the one responsible by this schema.
        This is the reason that this is marked as DynamicDocument, to prevent breaking changes
    """
    video_id = StringField(required=True, db_field="video_id")
    resolution = StringField(required=False)
    _media_type = StringField(required=True)
    upload_rules = ListField(field=EmbeddedDocumentField(DBRecordingRule), required=False)
    recording_overview = EmbeddedDocumentField(document_type=DBRecordingOverview, required=False)

    meta = {
        "collection": RECORDINGS_COLLECTION,
        "db_alias": "DataIngestionDB",
    }
