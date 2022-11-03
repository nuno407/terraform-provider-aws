"""Test processor module."""
import logging
from datetime import timedelta
from typing import Any, Dict, Union

from pytest import LogCaptureFixture

from base.processor import Processor


class SampleProcessor(Processor):
    """Sample processor class."""

    def _process(self,
                 _: Dict[timedelta, Dict[str, Union[bool, int, float]]]
                 ) -> Dict[str, Any]:
        _logger = logging.getLogger("mdfparser." + __name__)
        _logger.info("Expect this log message!")
        return {"sample": "processed"}

    @property
    def name(self):
        return "sample"


class TestProcessor:  # pylint: disable=too-few-public-methods
    """Test processor class."""

    def test_sample_processor(self, caplog: LogCaptureFixture):
        """Test sample processor"""
        # GIVEN
        data: Dict[timedelta, Dict[str, Union[bool, int, float]]] = {timedelta(minutes=5): {"foo": 2}}
        processor = SampleProcessor()
        caplog.set_level(logging.DEBUG)

        # WHEN
        result = processor.process(data)

        # THEN
        expected_data = {"sample": "processed"}
        assert result == expected_data
        assert "Starting processing with sample" in caplog.text
        assert "Expect this log message!" in caplog.text
        assert "Finished processing sample" in caplog.text
