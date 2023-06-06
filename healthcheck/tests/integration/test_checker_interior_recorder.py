"""Integration test module for interior recorder."""
import os
from datetime import datetime, timedelta

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType, S3VideoArtifact, TimeWindow
from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.controller.db import DatabaseController
from healthcheck.controller.voxel_fiftyone import VoxelFiftyOneController
from healthcheck.exceptions import (AnonymizedFileNotPresent,
                                    FailDocumentValidation,
                                    NotYetIngestedError, RawFileNotPresent,
                                    VoxelEntryNotPresent)
from healthcheck.s3_utils import S3Utils

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
S3_DATA = os.path.join(CURRENT_LOCATION, "data", "s3_data")


class TestInteriorRecorderArtifactChecker:
    @pytest.mark.integration
    @pytest.mark.parametrize("tenant_id,device_id,footage_id,footage_from,footage_to,expected_exception_type", [
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_f068e0fa-be30-5301-ba1b-293195554f9a_1669992746911_1669992825429
        # Success
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "f068e0fa-be30-5301-ba1b-293195554f9a",
            1669992746911,
            1669992825429,
            None
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_c3838d5d-6426-5267-9f10-82fad54f765b_1669992746911_1669992825530"
        # Error: Hash not present / not ingested by SDR yet
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "c3838d5d-6426-5267-9f10-82fad54f765b",
            1669992746911,
            1669992825530,
            NotYetIngestedError
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_a1f8422e-cfdf-5eb3-b6d4-6d784e36745b_1669992688000_1669992725794
        # Error: No video anonymized file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "a1f8422e-cfdf-5eb3-b6d4-6d784e36745b",
            1669992688000,
            1669992725794,
            AnonymizedFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_2aec57dc-9f07-5692-961b-903b359a7a01_1669989865000_1669989911443
        # Error: No CHC file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "2aec57dc-9f07-5692-961b-903b359a7a01",
            1669989865000,
            1669989911443,
            AnonymizedFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_131429bb-f3ae-5617-9a7d-e5b79444674d_1669941151635_1669941177428
        # Error: Missing original video
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "131429bb-f3ae-5617-9a7d-e5b79444674d",
            1669941151635,
            1669941177428,
            RawFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_676a8463-4b76-5681-bd9e-00574e56ef13_1669937546543_1669937582448
        # Error: Missing signals file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "676a8463-4b76-5681-bd9e-00574e56ef13",
            1669937546543,
            1669937582448,
            RawFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_bb4c4631-acbe-5807-810f-0c471d93b640_1669935745727_1669935774441
        # Error: Missing metadatafull file
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "bb4c4631-acbe-5807-810f-0c471d93b640",
            1669935745727,
            1669935774441,
            RawFileNotPresent
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_f2c93e50-7566-59d1-819a-1c6e59a85ade_1669933942000_1669933976440
        # Error: Missing voxel entry
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "f2c93e50-7566-59d1-819a-1c6e59a85ade",
            1669933942000,
            1669933976440,
            VoxelEntryNotPresent
        ),
        # DATANAUTS_DEV_01_InteriorRecorder_370d7ac9-55d6-5824-a878-4466f985a0c6_1668525233635_1668525352435
        # Success
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "370d7ac9-55d6-5824-a878-4466f985a0c6",
            1668525233635,
            1668525352435,
            None
        ),
        # DATANAUTS_DEV_01_InteriorRecorder_0d46bf3c-31fc-52fe-aae8-b8b7aaace851_1668520287819_1668520339524
        # Error: Missing CHC document in algorithm-output
        (
            "datanauts",
            "DATANAUTS_DEV_01",
            "0d46bf3c-31fc-52fe-aae8-b8b7aaace851",
            1668520287819,
            1668520339524,
            FailDocumentValidation
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1b4eba78-46dd-5920-9c8e-4ba55d937a02_1669920646000_1669920680442
        # Error: Missing processing_list in pipeline-execution
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "1b4eba78-46dd-5920-9c8e-4ba55d937a02",
            1669920646000,
            1669920680442,
            FailDocumentValidation
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_58d84c92-357c-59b9-bf8a-256b98dc4f77_1669924248635_1669924275444
        # # Error: Wrong media type in recordings
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "58d84c92-357c-59b9-bf8a-256b98dc4f77",
            1669924248635,
            1669924275444,
            FailDocumentValidation
        ),
        # ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_b8a210a3-557a-5f72-b673-e2b7bab750b8_1670008538727_1670008567439
        # Error: Missing document in signals
        (
            "deepsensation",
            "ivs_slimscaley_develop_yuj2hi_01",
            "b8a210a3-557a-5f72-b673-e2b7bab750b8",
            1670008538727,
            1670008567439,
            FailDocumentValidation
        )
    ])
    def test_run_healthcheck_interior(
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
        interior_recorder_artifact_checker = InteriorRecorderArtifactChecker(
            s3_controller=s3_utils,
            db_controller=database_controller,
            voxel_fiftyone_controller=voxel_fiftyone_controller
        )
        artifact = S3VideoArtifact(
            tenant_id=tenant_id,
            device_id=device_id,
            footage_id=footage_id,
            recorder=RecorderType.INTERIOR,
            timestamp=datetime.fromtimestamp(footage_from / 1000.0, tz=UTC),
            end_timestamp=datetime.fromtimestamp(footage_to / 1000.0, tz=UTC),
            upload_timing=TimeWindow(
                start=datetime.now(tz=UTC) - timedelta(hours=1),
                end=datetime.now(tz=UTC)),
            rcc_s3_path=f"s3://bucket/{tenant_id}/{device_id}/{footage_id}.mp4")

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            interior_recorder_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                interior_recorder_artifact_checker.run_healthcheck(artifact)
