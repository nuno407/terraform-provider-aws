""" Module containing common logic for service shutdown. """
import logging
import sys
from signal import SIGTERM, Signals, signal

_logger = logging.getLogger(__name__)


class ImmediateExit:  # pylint: disable=too-few-public-methods
    """ Class which encapsulates immediate service shutdown logic. """

    def __init__(self):
        _logger.info("Setting up immediate shutdown handler")
        signal(SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, _frame):
        """ Function that handles SIGTERM signal. """
        _logger.critical("Received termination request with signal %s. Exiting immediately.", Signals(signum).name)
        sys.exit()
