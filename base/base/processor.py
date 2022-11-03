""" Module that defines the base Processor class. """
import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, Dict, Union

_logger = logging.getLogger(__name__)


class Processor(ABC):
    """ Base class of a processor. """

    def process(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> Dict[str, Any]:
        """ Executes procssing logic. """
        _logger.debug("Starting processing with %s", self.name)
        result = self._process(synchronized_signals)
        _logger.debug("Finished processing %s", self.name)
        return result

    @abstractmethod
    def _process(self, synchronized_signals: Dict[timedelta, Dict[str, Union[bool, int, float]]]) -> Dict[str, Any]:
        """ Function to be implemented by the inheriting class which should contain the processing logic. """
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self):
        """ Name of the processor. """
