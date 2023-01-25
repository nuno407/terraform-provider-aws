"""Integration test module for interior recorder."""
import os
from datetime import datetime

import pytest

from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.controller.aws_s3 import S3Controller
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import (AnonymizedFileNotPresent,
                                    FailDocumentValidation,
                                    NotYetIngestedError, RawFileNotPresent,
                                    VoxelEntryNotPresent)
from healthcheck.model import VideoArtifact

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")


class TestInteriorRecorderArtifactChecker:
    @pytest.mark.integration
    @pytest.mark.parametrize("tenant_id,device_id,stream_name,footage_from,footage_to,expected_exception_type", [
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669992746911_1669992825429
        # Success
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669992746911,
            1669992825429,
            None
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669992746911_1669992825530"
        # Error: Hash not present / not ingested by SDR yet
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669992746911,
            1669992825530,
            NotYetIngestedError
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669992688000_1669992725794
        # Error: No video anonymized file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669992688000,
            1669992725794,
            AnonymizedFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669989865000_1669989911443
        # Error: No CHC file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669989865000,
            1669989911443,
            AnonymizedFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669941151635_1669941177428
        # Error: Missing original video
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669941151635,
            1669941177428,
            RawFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669937546543_1669937582448
        # Error: Missing signals file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669937546543,
            1669937582448,
            RawFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669935745727_1669935774441
        # Error: Missing metadatafull file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669935745727,
            1669935774441,
            RawFileNotPresent
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669933942000_1669933976440
        # Error: Missing voxel entry
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669933942000,
            1669933976440,
            VoxelEntryNotPresent
        ),
        # datanauts_DATANAUTS_DEV_01_InteriorRecorder_1668525233635_1668525352435
        # Success
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "datanauts_DATANAUTS_DEV_01_InteriorRecorder",
            1668525233635,
            1668525352435,
            None
        ),
        # datanauts_DATANAUTS_DEV_01_InteriorRecorder_1668520287819_1668520339524
        # Error: Missing CHC document in algorithm-output
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "datanauts_DATANAUTS_DEV_01_InteriorRecorder",
            1668520287819,
            1668520339524,
            FailDocumentValidation
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669920646000_1669920680442
        # Error: Missing processing_list in pipeline-execution
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669920646000,
            1669920680442,
            FailDocumentValidation
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669924248635_1669924275444
        # # Error: Wrong media type in recordings
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1669924248635,
            1669924275444,
            FailDocumentValidation
        ),
        # deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1670008538727_1670008567439
        # Error: Missing document in signals
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
            1670008538727,
            1670008567439,
            FailDocumentValidation
        )
    ])
    def test_run_healthcheck_interior(
            self,
            tenant_id: str,
            device_id: str,
            stream_name: str,
            footage_from: int,
            footage_to: int,
            expected_exception_type: Exception,
            database_controller: DatabaseController,
            blob_storage_controller: S3Controller,
            voxel_fiftyone_controller: VoxelFiftyOneController,
    ):
        """Test interior recorder healthcheck."""
        interior_recorder_artifact_checker = InteriorRecorderArtifactChecker(
            s3_controller=blob_storage_controller,
            db_controller=database_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )
        artifact = VideoArtifact(
            tenant_id=tenant_id,
            device_id=device_id,
            stream_name=stream_name,
            footage_from=datetime.fromtimestamp(footage_from / 1000.0),
            footage_to=datetime.fromtimestamp(footage_to / 1000.0))

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            interior_recorder_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                interior_recorder_artifact_checker.run_healthcheck(artifact)
