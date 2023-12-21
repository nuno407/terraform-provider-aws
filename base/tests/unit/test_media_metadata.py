"Test metadata artifacts"
from datetime import datetime
import pytest
import pytz
from base.model.metadata.media_metadata import MediaMetadata
from base.testing.utils import load_relative_str_file


def load_metadata_artifacts(file_name: str) -> str:
    """Load metadata artifacts"""
    path = f"artifacts/{file_name}"
    return load_relative_str_file(__file__, path)


class TestMediametadata:
    @pytest.mark.unit
    def test_snapshot_artifact_methods(self):
        """
        Test snapshot artifact
        """
        data = load_metadata_artifacts(
            "datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json")
        model = MediaMetadata.model_validate_json(data)

        assert isinstance(model, MediaMetadata)
        assert len(model.frames) != 0
        assert next(model.get_bool("isTest")).value
        assert next(model.get_float("CameraViewBlocked")).value == 0.948
        assert next(model.get_integer("intTest")).value == 1
        assert next(model.get_string("strTest")).value == "test"

    @pytest.mark.unit
    def test_media_artifact_timecalculation(self):
        """
        Test snapshot artifact
        """
        data = load_metadata_artifacts(
            "datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json")
        model = MediaMetadata.model_validate_json(data)

        assert isinstance(model, MediaMetadata)
        assert model.frames[0].timestamp == datetime(2023, 10, 11, 16, 5, 52, 902000, tzinfo=pytz.UTC)

    @pytest.mark.unit
    @pytest.mark.parametrize("snapshot_json", [
        (load_metadata_artifacts("TrainingMultiSnapshot_TrainingMultiSnapshot-9fa95e3c-2195-4850-98f2-7810adfe2ff0_1.jpeg.smart_data_1696352069031_cvb0.json")),
        (load_metadata_artifacts("datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json")),
        (load_metadata_artifacts("DATANAUTS_DEV_03_InteriorRecorder_01273c78-b3d5-4e81-8743-2f6c7689d5b9_1701106759743_1701106805454_metadata_full.json"))
    ], ids=["snapshot_pose", "snapshot_classifications", "video"])
    def test_general_metadata(self, snapshot_json: str):
        """
        Test snapshot artifact
        """
        model = MediaMetadata.model_validate_json(snapshot_json)

        assert isinstance(model, MediaMetadata)
        assert len(model.frames) != 0

    @pytest.mark.unit
    @pytest.mark.parametrize("snapshot_json", [
        (load_metadata_artifacts("DATANAUTS_DEV_03_InteriorRecorder_01273c78-b3d5-4e81-8743-2f6c7689d5b9_1701106759743_1701106805454_metadata_full.json")),
        (load_metadata_artifacts("datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json"))
    ], ids=["video", "snap"])
    def test_pose_metadata(self, snapshot_json: str):
        """
        Test snapshot artifact
        """

        model = MediaMetadata.model_validate_json(snapshot_json)

        assert isinstance(model, MediaMetadata)
        assert len(model.frames) != 0
        assert model.frames[0].object_list.person_details[0].keypoints[0].x == 1.1
