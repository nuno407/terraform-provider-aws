import os

from pytest import fixture

from selector.model import PreviewMetadataV063, parse

CURRENT_LOCATION = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
TEST_DATA_LOCATION = os.path.join(CURRENT_LOCATION, "test_data")


@fixture
def minimal_preview_metadata_str() -> str:
    with open(os.path.join(TEST_DATA_LOCATION, "minimal_preview_metadata.json")) as f:
        return f.read()


@fixture
def real_preview_metadata(real_preview_metadata_str: str) -> PreviewMetadataV063:
    return parse(real_preview_metadata_str)


@fixture
def minimal_preview_metadata(minimal_preview_metadata_str: str) -> PreviewMetadataV063:
    return parse(minimal_preview_metadata_str)
