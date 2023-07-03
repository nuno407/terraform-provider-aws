"""Integration test module for interior recorder."""
import os
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType, S3VideoArtifact, TimeWindow
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
    @pytest.mark.parametrize("tenant_id,device_id,footage_id,footage_from,footage_to,expected_exception_type", [
        # DATANAUTS_DEV_01_TrainingRecorder_2b9a313c-9b11-5ee8-a864-cb0b115a537e_1671454631000_1671455208714
        # Success
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "2b9a313c-9b11-5ee8-a864-cb0b115a537e",
            1671454631000,
            1671455208714,
            None
        ),
        # rc_srx_develop_stv4sf_01_TrainingRecorder_aa048c8e-68b3-5cc9-8114-d5a1c76ca1b5_1670846956000_1670847650634"
        # Error: data_status not completed / not ingested by SDR yet
        (
            "deepsensation",
            "rc_srx_develop_stv4sf_01",
            "aa048c8e-68b3-5cc9-8114-d5a1c76ca1b5",
            1670846956000,
            1670847650634,
            NotYetIngestedError
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_15afa52a-e3a1-522a-9d94-3b3a1cfbf400_1670831590000_1670831868817
        # Error: No video anonymized file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "15afa52a-e3a1-522a-9d94-3b3a1cfbf400",
            1670831590000,
            1670831868817,
            AnonymizedFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_854c58f5-ffac-52b2-8c96-bd75f5da978c_1670829790543_1670831591892
        # Error: No CHC file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "854c58f5-ffac-52b2-8c96-bd75f5da978c",
            1670829790543,
            1670831591892,
            AnonymizedFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_8335db64-2ce1-548c-bde1-3afd94efabbc_1670507347819_1670507467529
        # Error: Missing original video
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "8335db64-2ce1-548c-bde1-3afd94efabbc",
            1670507347819,
            1670507467529,
            RawFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_14400b23-dbfb-5d5a-9b6d-1cd4f8c48601_1670499585635_1670499861537
        # Error: Missing voxel entry
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "14400b23-dbfb-5d5a-9b6d-1cd4f8c48601",
            1670499585635,
            1670499861537,
            VoxelEntryNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_17c5b8b2-62f5-5497-9cf1-d8fdc73feab4_1670497785727_1670499585630
        # Error: Missing CHC document in algorithm-output
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "17c5b8b2-62f5-5497-9cf1-d8fdc73feab4",
            1670497785727,
            1670499585630,
            FailDocumentValidation
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_8ef1b5c7-dca9-5e7e-bbdb-eb01774b8cc9_1670494337819_1670494456614
        # Error: Missing pipeline-execution
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "8ef1b5c7-dca9-5e7e-bbdb-eb01774b8cc9",
            1670494337819,
            1670494456614,
            NotPresentError
        ),
        # ivs_slimscaley_develop_yuj2hi_01_TrainingRecorder_21cadbce-1120-5797-81ba-08da11754c33_1670492537000_1670494337793
        # # Error: Wrong media type in recordings
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "21cadbce-1120-5797-81ba-08da11754c33",
            1670492537000,
            1670494337793,
            FailDocumentValidation
        )
    ])
    def test_run_healthcheck(
            self,
            tenant_id: str,
            device_id: str,
            footage_id: str,
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
        artifact = S3VideoArtifact(
            tenant_id=tenant_id,
            device_id=device_id,
            footage_id=footage_id,
            recorder=RecorderType.TRAINING,
            timestamp=datetime.fromtimestamp(footage_from / 1000.0, tz=UTC),
            end_timestamp=datetime.fromtimestamp(footage_to / 1000.0, tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC) - timedelta(hours=1),
                end=datetime.now(tz=UTC)),
            rcc_s3_path=f"s3://rcc-bucket/{tenant_id}/{device_id}/{footage_id}")

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            training_recorder_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                training_recorder_artifact_checker.run_healthcheck(artifact)
