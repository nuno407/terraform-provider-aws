""" Module containing common logic for service shutdown. """
import logging
from signal import SIGTERM, Signals, signal

# inspired by https://stackoverflow.com/a/31464349

_logger = logging.getLogger(__name__)


class GracefulExit:
    """ Class which encapsulate service shutdown logic. """

    @property
    def continue_running(self):
        """ Variable that indicates if service should be shutdown or not. """
        return self.__continue_running

    def __init__(self):
        self.__continue_running = True
        signal(SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, _frame):
        """ Function that handles SIGTERM signal. """
        _logger.critical(
            "Received termination request with signal %s. Trying to shutdown gracefully.",
            Signals(signum).name)
        self.__continue_running = False
