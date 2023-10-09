from base.model.base_model import S3Path, ConfiguredBaseModel
from typing import Optional
import pytest


class MockArtifact(ConfiguredBaseModel):
    attribute_1: S3Path
    attribute_2: Optional[S3Path] = None

class TestBaseModel:

    @pytest.mark.unit
    @pytest.mark.parametrize("artifact,expected_artifact", [
        (
            """
            {
                "attribute_1" : "s3://bucket/file"
            }
            """,
            MockArtifact(attribute_1="s3://bucket/file", attribute_2=None)
        ),
        (
            """
            {
                "attribute_1" : "s3://bucket/file",
                "attribute_2" : "s3://bucket/file2"
            }
            """,
            MockArtifact(attribute_1="s3://bucket/file", attribute_2="s3://bucket/file2")
        ),
        (
            """
            {
                "attribute_1" : "s3://bucket/file"
            }
            """,
            MockArtifact(attribute_1="s3://bucket/file", attribute_2=None)
        )
    ], ids=["1", "2", "3"])
    def test_s3_path_success(self, artifact: str, expected_artifact: ConfiguredBaseModel):
        assert MockArtifact.model_validate_json(artifact) == expected_artifact

    @pytest.mark.unit
    def test_s3_path_fail_needed_field(self):

        with pytest.raises(Exception) as f:
            var = MockArtifact()

    @pytest.mark.unit
    @pytest.mark.parametrize("s3_path", [
        (
            "/bucket/file"
        ),
        (
            "s3://bucket"
        ),
        (
            "s3:/bucket/file"
        ),
        (
            None
        )
    ])
    def test_s3_path_fail_wrong_s3_path(self, s3_path: str):

        with pytest.raises(Exception) as f:
            var = MockArtifact(attribute_1=s3_path, attribute_2=None)
