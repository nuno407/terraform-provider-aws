"""Test processor module."""
import logging
from datetime import timedelta
from typing import Any, Dict, Union
import unittest

import pytest

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


@pytest.mark.unit
class TestProcessor:  # pylint: disable=too-few-public-methods
    """Test processor class."""

    @unittest.mock.patch("base.processor._logger")
    def test_sample_processor(self, mock_logger):
        """Test sample processor"""
        # GIVEN
        data: Dict[timedelta, Dict[str, Union[bool, int, float]]] = {timedelta(minutes=5): {"foo": 2}}
        processor = SampleProcessor()

        # WHEN
        result = processor.process(data)

        # THEN
        expected_data = {"sample": "processed"}
        assert result == expected_data
        mock_logger.debug.assert_any_call("Starting processing with %s", "sample")
        mock_logger.debug.assert_any_call("Finished processing %s", "sample")
