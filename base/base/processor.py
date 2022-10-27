import logging
from abc import ABC
from abc import abstractmethod
from abc import abstractproperty
from datetime import timedelta
from typing import Any
from typing import Dict
from typing import Union

_logger = logging.getLogger(__name__)


class Processor(ABC):
    def process(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> Dict[str, Any]:
        _logger.debug('Starting processing with %s', self.name)
        result = self._process(synchronized_signals)
        _logger.debug('Finished processing %s', self.name)
        return result

    @abstractmethod
    def _process(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> Dict[str, Any]:
        raise NotImplementedError()

    @abstractproperty
    def name(self):
        pass
