import pytest
from healthcheck.model import VideoArtifact, ArtifactType
from datetime import datetime

@pytest.mark.unit
class TestVideoArtifactModelType:
    @pytest.mark.parametrize("input_artifact,expected_type", [
        (
            VideoArtifact(
                tenant_id="tenant1",
                device_id="device1",
                stream_name="stream1_InteriorRecorder",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            ArtifactType.INTERIOR_RECORDER
        ),
        (
            VideoArtifact(
                tenant_id="tenant2",
                device_id="device2",
                stream_name="stream1_TrainingRecorder",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            ArtifactType.TRAINING_RECORDER
        ),
        (
            VideoArtifact(
                tenant_id="tenant2",
                device_id="device2",
                stream_name="stream1_FrontRecorder",
                footage_from=datetime.now(),
                footage_to=datetime.now()
            ),
            ArtifactType.FRONT_RECORDER
        )
    ])
    def test_video_artifact_type(self, input_artifact: VideoArtifact, expected_type: ArtifactType):
        assert input_artifact.artifact_type == expected_type
