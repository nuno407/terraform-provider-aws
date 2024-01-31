"""Tests for mongodb controller related to IMU ingestion"""

from datetime import datetime
from typing import Union
from unittest.mock import AsyncMock, MagicMock
from pytest import mark, raises
from base.model.artifacts import (CameraBlockedOperatorArtifact,
                                  PeopleCountOperatorArtifact,
                                  SOSOperatorArtifact, OperatorArtifact)
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.exceptions import InvalidOperatorArtifactException


def _generate_operator_artifact() -> dict:
    event_timestamp = datetime.fromisoformat("2023-08-29T08:17:15+00:00")
    operator_monitoring_start = datetime.fromisoformat("2023-08-29T08:18:49+00:00")
    operator_monitoring_end = datetime.fromisoformat("2023-08-29T08:35:57+00:00")

    artifact = {
        "tenant_id": "datanauts",
        "device_id": "DATANAUTS_DEV_02",
        "event_timestamp": event_timestamp,
        "operator_monitoring_start": operator_monitoring_start,
        "operator_monitoring_end": operator_monitoring_end,
        "additional_information": {
            "is_door_blocked": True,
            "is_camera_blocked": True,
            "is_audio_malfunction": True,
            "observations": "foo"
        }
    }
    return artifact


def _generate_people_count_operator_artifact():

    artifact = _generate_operator_artifact()
    artifact["artifact_name"] = "sav-operator-people-count"
    artifact["is_people_count_correct"] = True
    artifact["correct_count"] = 3
    parsed_artifact = PeopleCountOperatorArtifact.model_validate(artifact)
    return parsed_artifact


def _generate_sos_operator_artifact():

    artifact = _generate_operator_artifact()
    artifact["artifact_name"] = "sav-operator-sos"
    artifact["reason"] = "ACCIDENTAL"
    parsed_artifact = SOSOperatorArtifact.model_validate(artifact)
    return parsed_artifact


def _generate_camera_blocked_operator_artifact():

    artifact = _generate_operator_artifact()
    artifact["artifact_name"] = "sav-operator-camera-blocked"
    artifact["is_chc_correct"] = True
    parsed_artifact = CameraBlockedOperatorArtifact.model_validate(artifact)
    return parsed_artifact


def _generate_bad_operator_artifact():
    artifact = _generate_operator_artifact()
    artifact["artifact_name"] = "sav-operator"
    parsed_artifact = OperatorArtifact.model_validate(artifact)
    return parsed_artifact


@mark.unit
class TestMongoControllerOperator:
    """Class with tests for mongodb controller related to operator feedback artifacts"""
    @mark.parametrize("artifact", [_generate_camera_blocked_operator_artifact(),
                                   _generate_sos_operator_artifact(),
                                   _generate_people_count_operator_artifact()])
    async def test_create_sav_operator(self, mongo_controller: MongoService, operator_feedback_engine: MagicMock,
                                       artifact: Union[SOSOperatorArtifact, CameraBlockedOperatorArtifact,
                                                       PeopleCountOperatorArtifact]):
        """Test for proces_imu_artifact method

        Args:
            mongo_controller (MongoController): class where the tested method is defined
            operator_feedback_engine (MagicMock): operator feedback engine mock
            artifact (Union[SOSOperatorArtifact, CameraBlockedOperatorArtifact,
                      PeopleCountOperatorArtifact]): valid operator artifact
        """
        # GIVEN
        operator_feedback_engine.save = AsyncMock()

        # WHEN
        await mongo_controller.create_operator_feedback_event(artifact)

        # THEN
        operator_feedback_engine.save.assert_called_once_with(artifact)

    @mark.parametrize("artifact", [_generate_bad_operator_artifact()])
    async def test_create_sav_operator_invalid(self, mongo_controller: MongoService,
                                               operator_feedback_engine: MagicMock, artifact: OperatorArtifact):
        """Test for create_sav_operator method with invalid artifact

        Args:
            mongo_controller (MongoController): class where the tested method is defined
            operator_feedback_engine (MagicMock): operator feedback engine
            artifact (OperatorArtifact): valid operator artifact, but not an instance of any of its subclasses
        """
        # GIVEN
        operator_feedback_engine.save = AsyncMock()

        # WHEN
        with raises(InvalidOperatorArtifactException):
            await mongo_controller.create_operator_feedback_event(artifact)

        # THEN
        operator_feedback_engine.save.assert_not_called()
