from base.model.artifacts import (OperatorArtifact, SOSOperatorArtifact, PeopleCountOperatorArtifact,
                                  CameraBlockedOperatorArtifact, OperatorAdditionalInformation)
from metadata.consumer.database.operator_feedback import DBSOSOperatorArtifact, DBOperatorAdditionalInformation, \
    DBPeopleCountOperatorArtifact, DBCameraBlockedOperatorArtifact

class OperatorRepository:
    """Operator Repository to translate model artifacts to database models"""

    @staticmethod
    def get_additional_information(additional_information: OperatorAdditionalInformation):
        """
        Get database version of additional information
        Args:
            additional_information: Artifact model to translate

        Returns:

        """
        return DBOperatorAdditionalInformation(
            is_door_blocked=additional_information.is_door_blocked,
            is_camera_blocked=additional_information.is_camera_blocked,
            is_audio_malfunction=additional_information.is_audio_malfunction,
            observations=additional_information.observations
        )

    @staticmethod
    def create_operator_feedback(artifact: OperatorArtifact):
        """
        Create operator feedback entry in database
        Args:
            artifact: The artifact to store
        """

        metadata = {
            "tenant_id": artifact.tenant_id,
            "device_id": artifact.device_id,
            "operator_monitoring_start": artifact.operator_monitoring_start,
            "operator_monitoring_end": artifact.operator_monitoring_end,
            "event_timestamp": artifact.event_timestamp,
            "artifact_name": artifact.artifact_name,
        }

        if isinstance(artifact, SOSOperatorArtifact):
            additional_information = OperatorRepository.get_additional_information(artifact.additional_information)
            db_artifact = DBSOSOperatorArtifact(reason=artifact.reason,
                                                additional_information=additional_information, **metadata)
            db_artifact.save()
            return
        if isinstance(artifact, PeopleCountOperatorArtifact):
            additional_information = OperatorRepository.get_additional_information(artifact.additional_information)
            db_artifact = DBPeopleCountOperatorArtifact(is_people_count_correct=artifact.is_people_count_correct,
                                                        additional_information=additional_information, **metadata)
            if artifact.correct_count is not None:
                db_artifact.correct_count = artifact.correct_count
            db_artifact.save()
            return
        if isinstance(artifact, CameraBlockedOperatorArtifact):
            additional_information = OperatorRepository.get_additional_information(artifact.additional_information)
            db_artifact = DBCameraBlockedOperatorArtifact(is_chc_correct=artifact.is_chc_correct,
                                                          additional_information=additional_information, **metadata)
            db_artifact.save()
            return
