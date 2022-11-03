import logging
from signal import SIGTERM, Signals, signal

_logger = logging.getLogger(__name__)

# inspired by https://stackoverflow.com/a/31464349


class GracefulExit:
    @property
    def continue_running(self):
        return self.__continue_running

    def __init__(self):
        self.__continue_running = True
        signal(SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        _logger.critical(
            "Received termination request with signal %s. Trying to shutdown gracefully.",
            Signals(signum).name)
        self.__continue_running = False
