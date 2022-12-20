"""Integration test module for interior recorder."""
import pytest
from healthcheck.exceptions import (
    AnonymizedFileNotPresent,
    RawFileNotPresent,
    VoxelEntryNotPresent,
    FailDocumentValidation)
from datetime import datetime
from healthcheck.checker.interior_recorder import InteriorRecorderArtifactChecker
from healthcheck.model import VideoArtifact


@pytest.mark.usefixtures("interior_recorder_artifact_checker")
class TestInteriorRecorderArtifactChecker:
    # FIXME something wrong with moto3, is always returning 404 on the S3 objects
    @pytest.mark.skip
    @pytest.mark.integration
    @pytest.mark.parametrize("artifact_id,device_id,stream_name,footage_from,footage_to,expected_exception_type", [
        # Success
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669992746911_1669992825429",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669992746911,
         1669992825429,
         None),
        # Error: No video anonymized file
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669992688000_1669992725794",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669992688000,
         1669992725794,
         AnonymizedFileNotPresent),
        # Error: No CHC file
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669989865000_1669989911443",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669989865000,
         1669989911443,
         AnonymizedFileNotPresent),
        # Error: Missing original video
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669941151635_1669941177428",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669941151635,
         1669941177428,
         RawFileNotPresent),
        # Error: Missing signals file
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669937546543_1669937582448",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669937546543,
         1669937582448,
         RawFileNotPresent),
        # Error: Missing metadatafull file
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669935745727_1669935774441",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669935745727,
         1669935774441,
         RawFileNotPresent),
        # Error: Missing voxel entry
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669933942000_1669933976440",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669933942000,
         1669933976440,
         VoxelEntryNotPresent),
        ("datanauts_DATANAUTS_DEV_01_InteriorRecorder_1668525233635_1668525352435",
         "DATANAUTS_DEV_01",
         "DATANAUTS_DEV_01_InteriorRecorder",
         1668525233635,
         1668525352435,
         None),
        # Error: Missing CHC document in algorithm-output
        ("datanauts_DATANAUTS_DEV_01_InteriorRecorder_1668520287819_1668520339524",
         "DATANAUTS_DEV_01",
         "DATANAUTS_DEV_01_InteriorRecorder",
         1668520287819,
         1668520339524,
         FailDocumentValidation),
        # Error: Missing processing_list in pipeline-execution
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669920646000_1669920680442",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669920646000,
         1669920680442,
         FailDocumentValidation),
        # Error: Wrong media type in recordings
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1669924248635_1669924275444",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1669924248635,
         1669924275444,
         FailDocumentValidation),
        # # Error: Missing document in signals
        ("deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1670008538727_1670008567439",
         "ivs_slimscaley_develop_yuj2hi_01",
         "ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder",
         1670008538727,
         1670008567439,
         FailDocumentValidation)
    ])
    def test_run_healthcheck_interior(
            self,
            artifact_id: str,
            device_id: str,
            stream_name: str,
            footage_from: int,
            footage_to: int,
            expected_exception_type: Exception,
            interior_recorder_artifact_checker: InteriorRecorderArtifactChecker,
    ):
        """Test interior recorder healthcheck."""
        artifact = VideoArtifact(
            artifact_id,
            device_id,
            stream_name,
            datetime.fromtimestamp(footage_from/ 1000.0),
            datetime.fromtimestamp(footage_to/ 1000.0))

        # Make sure no exception is raised if None is provided
        if expected_exception_type is None:
            interior_recorder_artifact_checker.run_healthcheck(artifact)
            assert True

        # Make sure an exception is raised if provided
        else:
            with pytest.raises(expected_exception_type):  # type: ignore
                interior_recorder_artifact_checker.run_healthcheck(artifact)
