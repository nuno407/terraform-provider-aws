from datetime import timedelta
import logging
from typing import Any, Union

from pytest import LogCaptureFixture
from base.processor import Processor


class SampleProcessor(Processor):
    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->dict[str, Any]:
        _logger = logging.getLogger('mdfparser.' + __name__)
        _logger.info('Expect this log message!')
        return {'sample': 'processed'}

    @property
    def name(self):
        return 'sample'

class TestProcessor:
    def test_sample_processor(self, caplog: LogCaptureFixture):
        # GIVEN
        data = { timedelta(minutes=5): {'foo': 2} }
        processor = SampleProcessor()
        caplog.set_level(logging.DEBUG)

        # WHEN
        result = processor.process(data)

        # THEN
        expected_data = {'sample': 'processed'}
        assert(result == expected_data)
        assert('Starting processing with sample' in caplog.text)
        assert('Expect this log message!' in caplog.text)
        assert('Finished processing sample' in caplog.text)
