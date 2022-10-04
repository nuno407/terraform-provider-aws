from abc import ABC, abstractmethod, abstractproperty
from datetime import timedelta
import logging
from typing import Any, Union

_logger = logging.getLogger(__name__)

class Processor(ABC):
    def process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->dict[str, Any]:
        _logger.debug('Starting processing with %s', self.name)
        result = self._process(synchronized_signals)
        _logger.debug('Finished processing %s', self.name)
        return result

    @abstractmethod
    def _process(self, synchronized_signals: dict[timedelta, dict[str, Union[bool, int, float]]])->dict[str, Any]:
        pass

    @abstractproperty
    def name(self):
        pass
