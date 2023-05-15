""" Test post processor module """
from unittest import mock

import pytest
from sdretriever.ingestor.post_processor import FFProbeExtractorPostProcessor


@pytest.mark.unit
def test_post_processor():
    ffmpeg_post_processor = FFProbeExtractorPostProcessor()

    with mock.patch("sdretriever.ingestor.post_processor.subprocess.run") as subprocess_run:
        subprocess_run.return_value = mock.Mock(
            stdout=b'{"format": {"duration": "1.000000"}, "streams": [{"width": 1920, "height": 1080}]}')
        video_info = ffmpeg_post_processor.execute(b'video_bytes')
        assert video_info.duration == 1.0
        assert video_info.width == 1920
        assert video_info.height == 1080
