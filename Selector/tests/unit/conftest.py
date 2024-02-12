# autopep8: off
from unittest.mock import Mock
from typing import cast
from pytest import fixture
import os
# needs to be set before importing the selector.model module
os.environ["MONGO_RECORDINGS_COLLECTION"] = "test-recordings"
from selector.model import PreviewMetadataV063, parse  # noqa: E402
from selector.model.recordings import Recordings
# autopep8: on
CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA_LOCATION = os.path.join(CURRENT_LOCATION, "test_data")


@fixture
def mock_recordings() -> Recordings:
    return Mock(spec=Recordings)


@fixture
def minimal_preview_metadata_str() -> str:
    with open(os.path.join(TEST_DATA_LOCATION, "minimal_preview_metadata.json")) as f:
        return f.read()


@fixture
def real_preview_metadata(real_preview_metadata_str: str) -> PreviewMetadataV063:
    return cast(PreviewMetadataV063, parse(real_preview_metadata_str))


@fixture
def minimal_preview_metadata(minimal_preview_metadata_str: str) -> PreviewMetadataV063:
    return cast(PreviewMetadataV063, parse(minimal_preview_metadata_str))
