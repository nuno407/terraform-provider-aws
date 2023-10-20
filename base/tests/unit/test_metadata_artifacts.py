"Test metadata artifacts"
import pytest
from base.model.metadata.snapshot_metadata import SnapshotMetadata
from base.testing.utils import load_relative_str_file


def load_metadata_artifacts(file_name: str) -> str:
    """Load metadata artifacts"""
    path = f"artifacts/{file_name}"
    return load_relative_str_file(__file__, path)


class TestMetadataArtifacts:

    @pytest.mark.unit
    @pytest.mark.parametrize("snapshot_json", [
        (load_metadata_artifacts("TrainingMultiSnapshot_TrainingMultiSnapshot-9fa95e3c-2195-4850-98f2-7810adfe2ff0_1.jpeg.smart_data_1696352069031_cvb0.json")),
        (load_metadata_artifacts("datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json"))
    ], ids=["pose", "classifications"])
    def test_snapshot_artifact(self, snapshot_json: str):
        """
        Test snapshot artifact
        """
        model = SnapshotMetadata.model_validate_json(snapshot_json)

        assert isinstance(model, SnapshotMetadata)
        assert len(model.frames) != 0

    @pytest.mark.unit
    def test_snapshot_artifact_methods(self):
        """
        Test snapshot artifact
        """
        data = load_metadata_artifacts(
            "datanauts_DATANAUTS_TEST_01_TrainingMultiSnapshot_TrainingMultiSnapshot-1fb80ea2-7460-4388-8f96-b7676b36ff94_1_1697040352931_metadata_full.json")
        model = SnapshotMetadata.model_validate_json(data)

        assert isinstance(model, SnapshotMetadata)
        assert len(model.frames) != 0
        assert next(model.get_bool("isTest")).value
        assert next(model.get_float("CameraViewBlocked")).value == 0.948
        assert next(model.get_integer("intTest")).value == 1
        assert next(model.get_string("strTest")).value == "test"
