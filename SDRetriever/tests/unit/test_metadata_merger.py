# type: ignore
from typing import Any
import pytest
import os
from sdretriever.metadata_merger import MetadataMerger
from sdretriever.ingestor.metacontent import MetacontentChunk

from base.testing.utils import load_relative_raw_file, load_relative_json_file


def helper_load_raw_metadata(filename: str) -> bytes:
    """Load raw metadata files using relative path"""
    return load_relative_raw_file(__file__, os.path.join("data", "raw_metadata_chunks", filename))


def helper_load_merged_metadata(filename: str) -> dict[Any, Any]:
    """Load merged metadata files using relative path"""
    return load_relative_json_file(__file__, os.path.join(
        "data", "merged_metadata_chunks", filename))


class TestMetadataMerger:

    @pytest.fixture
    def metadata_merger(self) -> MetadataMerger:
        return MetadataMerger()

    @pytest.mark.unit
    @pytest.mark.parametrize("chunks,expected_merge", [
        (
            [
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_2.jpeg.interior_20230609_070327_902_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_3.jpeg.interior_20230609_070357_901_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_4.jpeg.interior_20230609_070427_901_metadata.json")
            ],
            helper_load_merged_metadata("preview_recorder_merged.json")
        ),
        # Test file without frames
        (
            [
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_1.jpeg.interior_20230609_070257_901_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_2.jpeg.interior_20230609_070327_902_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_3.jpeg.interior_20230609_070357_901_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_4.jpeg.interior_20230609_070427_901_metadata.json")
            ],
            helper_load_merged_metadata("preview_recorder_merged.json")
        ),
        # Test file with empty frames
        (
            [
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_1.jpeg.interior_20230609_070257_903_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_2.jpeg.interior_20230609_070327_902_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_3.jpeg.interior_20230609_070357_901_metadata.json"),
                helper_load_raw_metadata(
                    "InteriorRecorderPreview_InteriorRecorderPreview-30bbcac1-a551-43f9-bda0-edac6ec39e76_4.jpeg.interior_20230609_070427_901_metadata.json")
            ],
            helper_load_merged_metadata("preview_recorder_merged.json")
        )
    ])
    def test_merge_metadata_chunks(
            self, chunks: list[bytes], expected_merge: dict[Any, Any], metadata_merger: MetadataMerger):

        # GIVEN
        metacontent_chunks = [MetacontentChunk(data, "MOCK") for data in chunks]

        # WHEN
        merged_data = metadata_merger.merge_metadata_chunks(metacontent_chunks)

        print(merged_data)
        print()
        print(expected_merge)

        # THEN
        assert merged_data == expected_merge
