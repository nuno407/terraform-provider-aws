"""
Initial python test
"""

import pytest
from sdm.main import identify_file,FileMetadata

TEST_VIDEO_ID_FIXTURE = "ridecare_companion_trial_rc_srx_prod_5bfe33136f2a43afc3f534f535d402af175914e2_InteriorRecorder_1667387817727_1667388470705"


@pytest.mark.unit
@pytest.mark.parametrize("s3_path,expect_metadata", [
    ("mp4", FileMetadata(None, None, None)),
    (TEST_VIDEO_ID_FIXTURE, FileMetadata(None, f"{TEST_VIDEO_ID_FIXTURE}", None)),
    (f"{TEST_VIDEO_ID_FIXTURE}.mp4", FileMetadata(None, f"{TEST_VIDEO_ID_FIXTURE}.mp4", ".mp4")),
    (f"Debug_Lync/{TEST_VIDEO_ID_FIXTURE}.mp4", FileMetadata("Debug_Lync", f"{TEST_VIDEO_ID_FIXTURE}.mp4", ".mp4"))
])
def test_identify_file(s3_path: str, expect_metadata: FileMetadata):
    # WHEN
    got_metadata = identify_file(s3_path)

    # THEN
    assert got_metadata == expect_metadata, "Method invocation result is not as expected"
