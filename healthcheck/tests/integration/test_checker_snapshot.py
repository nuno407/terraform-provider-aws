"""Integration test module for interior recorder."""
from datetime import datetime

import os
import pytest

from healthcheck.checker.snapshot import \
    SnapshotArtifactChecker
from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import (AnonymizedFileNotPresent,
                                    FailDocumentValidation, RawFileNotPresent, NotYetIngestedError,
                                    VoxelEntryNotPresent)
from healthcheck.model import SnapshotArtifact

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")


class TestSnapshotArtifactChecker:
    @pytest.mark.integration
    @pytest.mark.parametrize("tenant_id,device_id,uuid,timestamp,expected_exception_type", [
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_9_1670829948622
        # Success
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_9",
            1670829948622,
            None
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_11_1670830350607
        # Error: data status not completed / not ingested by SDR yet
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_11",
            1670830350607,
            NotYetIngestedError
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_14_1670830952628
        # Error: No anonymized file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-221ccc49-d2ff-4588-8796-5f980ff2c0e6_14",
            1670830952628,
            AnonymizedFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingMultiSnapshot_TrainingMultiSnapshot-62d74bec-f590-4056-87c4-71ca2ffb98d0_3_1670506145053
        # Error: Missing original video
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-62d74bec-f590-4056-87c4-71ca2ffb98d0_3",
            1670506145053,
            RawFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_TrainingMultiSnapshot_TrainingMultiSnapshot-62d74bec-f590-4056-87c4-71ca2ffb98d0_8_1670507150015
        # Error: Missing voxel entry
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-62d74bec-f590-4056-87c4-71ca2ffb98d0_8",
            1670507150015,
            VoxelEntryNotPresent
        ),
        # deepsensation_rc_srx_develop_stv4sf_01_TrainingMultiSnapshot_TrainingMultiSnapshot-ed93315c-4b73-45ef-936d-4885dee293ae_5_1670432192376
        # Error: Does not contain corresponsing interior recorder
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "TrainingMultiSnapshot_TrainingMultiSnapshot-ed93315c-4b73-45ef-936d-4885dee293ae_5",
            1670432192376,
            NotYetIngestedError
        )
    ])
    def test_run_healthcheck_snapshot(
            self,
            tenant_id: str,
            device_id: str,
            uuid: str,
            timestamp: int,
            expected_exception_type: Exception,
            database_controller: DatabaseController,
            blob_storage_controller: S3Controller,
            voxel_fiftyone_controller: VoxelFiftyOneController,
    ):
        snapshot_artifact_checker = SnapshotArtifactChecker(
            s3_controller=blob_storage_controller,
            db_controller=database_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )
        """Test interior recorder healthcheck."""
        artifact = SnapshotArtifact(
            tenant_id=tenant_id,
            device_id=device_id,
            uuid=uuid,
            timestamp=datetime.fromtimestamp(timestamp / 1000.0))

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            snapshot_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                snapshot_artifact_checker.run_healthcheck(artifact)
