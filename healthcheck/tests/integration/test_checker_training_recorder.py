"""Integration test module for interior recorder."""
import os
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType, TimeWindow, VideoArtifact
from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import (AnonymizedFileNotPresent,
                                    FailDocumentValidation, NotPresentError,
                                    NotYetIngestedError, RawFileNotPresent,
                                    VoxelEntryNotPresent)
from healthcheck.s3_utils import S3Utils

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")


class TestTrainingRecorderArtifactChecker:
    @pytest.mark.integration
    @pytest.mark.parametrize("tenant_id,device_id,stream_name,footage_from,footage_to,expected_exception_type", [
        # datanauts_DATANAUTS_DEV_01_TrainingRecorder_1671454631000_1671455208714
        # Success
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "datanauts_DATANAUTS_DEV_01_TrainingRecorder",
            1671454631000,
            1671455208714,
            None
        ),
        # deepsensation_rc_srx_develop_stv4sf_01_TrainingRecorder_1670846956000_1670847650634"
        # Error: data_status not completed / not ingested by SDR yet
        (
            "deepsensation",
            "rc_srx_develop_stv4sf_01",
            "deepsensation_rc_srx_develop_stv4sf_01_TrainingRecorder",
            1670846956000,
            1670847650634,
            NotYetIngestedError
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670831590000_1670831868817
        # Error: No video anonymized file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670831590000,
            1670831868817,
            AnonymizedFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670829790543_1670831591892
        # Error: No CHC file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670829790543,
            1670831591892,
            AnonymizedFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670507347819_1670507467529
        # Error: Missing original video
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670507347819,
            1670507467529,
            RawFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670499585635_1670499861537
        # Error: Missing voxel entry
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670499585635,
            1670499861537,
            VoxelEntryNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670497785727_1670499585630
        # Error: Missing CHC document in algorithm-output
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670497785727,
            1670499585630,
            FailDocumentValidation
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670494337819_1670494456614
        # Error: Missing pipeline-execution
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670494337819,
            1670494456614,
            NotPresentError
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_1670492537000_1670494337793
        # # Error: Wrong media type in recordings
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder",
            1670492537000,
            1670494337793,
            FailDocumentValidation
        )
    ])
    def test_run_healthcheck(
            self,
            tenant_id: str,
            device_id: str,
            stream_name: str,
            footage_from: int,
            footage_to: int,
            expected_exception_type: Exception,
            database_controller: DatabaseController,
            s3_utils: S3Utils,
            voxel_fiftyone_controller: VoxelFiftyOneController,
    ):
        """Test interior recorder healthcheck."""

        training_recorder_artifact_checker = TrainingRecorderArtifactChecker(
            s3_utils=s3_utils,
            db_controller=database_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )
        artifact = VideoArtifact(
            tenant_id=tenant_id,
            device_id=device_id,
            stream_name=stream_name,
            recorder=RecorderType.TRAINING,
            timestamp=datetime.fromtimestamp(footage_from / 1000.0, tz=UTC),
            end_timestamp=datetime.fromtimestamp(footage_to / 1000.0, tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC) - timedelta(hours=1),
                end=datetime.now(tz=UTC)))

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            training_recorder_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                training_recorder_artifact_checker.run_healthcheck(artifact)
