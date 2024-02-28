from mongoengine import Document, StringField, DateTimeField, BooleanField, EmbeddedDocumentField, EnumField, IntField, \
    EmbeddedDocument
from base.model.artifacts import OperatorSOSReason
from kink import di


class DBOperatorAdditionalInformation(EmbeddedDocument):
    """ Database embedded document OperatorAdditionalInformation"""
    is_door_blocked = BooleanField(required=True)
    is_camera_blocked = BooleanField(required=True)
    is_audio_malfunction = BooleanField(required=True)
    observations = StringField(required=False)


class DBOperatorFeedback(Document):
    """ Database base document OperatorFeedback"""
    tenant_id = StringField(required=True)
    device_id = StringField(required=True)
    operator_monitoring_start = DateTimeField(required=True)
    operator_monitoring_end = DateTimeField(required=True)
    event_timestamp = DateTimeField(required=True)
    artifact_name = StringField(required=True)

    meta = {
        "collection": di["db_metadata_tables"]["sav_operator_feedback"],
        "allow_inheritance": True,
        "db_alias": "DataIngestionDB"
    }


class DBSOSOperatorArtifact(DBOperatorFeedback):
    """Database document SOSOperatorArtifact"""
    additional_information = EmbeddedDocumentField(DBOperatorAdditionalInformation, required=True)
    reason = EnumField(OperatorSOSReason, required=True)


class DBOtherOperatorArtifact(DBOperatorFeedback):
    """Database document SOSOperatorArtifact"""
    additional_information = EmbeddedDocumentField(DBOperatorAdditionalInformation, required=True)
    type = StringField(required=True)


class DBPeopleCountOperatorArtifact(DBOperatorFeedback):
    """Database document PeopleCountOperatorArtifact"""
    additional_information = EmbeddedDocumentField(DBOperatorAdditionalInformation, required=True)
    is_people_count_correct = BooleanField(required=True)
    correct_count = IntField(required=False)


class DBCameraBlockedOperatorArtifact(DBOperatorFeedback):
    """Database document CameraBlockedOperatorArtifact"""
    additional_information = EmbeddedDocumentField(DBOperatorAdditionalInformation, required=True)
    is_chc_correct = BooleanField(required=True)
