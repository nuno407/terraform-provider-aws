"""Integration test module for interior recorder."""
import os
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import (RecorderType, Recording, SOSOperatorArtifact,
                                  TimeWindow)
from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.checker.sav_operator_events import SAVOperatorArtifactChecker
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import NotPresentError
from healthcheck.s3_utils import S3Utils

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")


class TestInteriorRecorderArtifactChecker:
    @pytest.mark.integration
    @pytest.mark.parametrize("tenant_id,expected_exception_type", [
        # Success
        (
            "datanauts",
            None
        ),
        # Error: Entry not present in DB
        (
            "datacats",
            NotPresentError
        )
    ])
    def test_run_healthcheck_operator(
            self,
            tenant_id: str,
            expected_exception_type: Exception,
            database_controller: DatabaseController,
    ):
        """Test interior recorder healthcheck."""
        operator_artifact_checker = SAVOperatorArtifactChecker(
            db_controller=database_controller,
        )
        artifact = SOSOperatorArtifact(
            tenant_id=tenant_id,
            device_id="DATANAUTS_DEV_02",
            event_timestamp=datetime(2023, 1, 1, tzinfo=UTC),
            operator_monitoring_start=datetime(2023, 1, 1, tzinfo=UTC),
            operator_monitoring_end=datetime(2023, 1, 1, tzinfo=UTC),
            reason="ACCIDENTAL",
            additional_information={
                "is_door_blocked": True,
                "is_camera_blocked": True,
                "is_audio_malfunction": True,
                "observations": "This is just a fancy integration test."
            }
        )

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            operator_artifact_checker.run_healthcheck(artifact)

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                operator_artifact_checker.run_healthcheck(artifact)
