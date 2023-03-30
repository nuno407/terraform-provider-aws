# type: ignore
""" main module. """
import logging
import time
from kink import inject

from sanitizer.bootstrap import bootstrap_di
from sanitizer.handler import Handler

_logger = logging.getLogger(__name__)


@inject
def main(handler: Handler, start_delay: int):
    """ main function. """
    _logger.info("Starting sanitizer...")
    if start_delay > 0:
        _logger.info("Waiting for %s seconds to start...", start_delay)
        time.sleep(start_delay)
    handler.run()


if __name__ == "__main__":
    bootstrap_di()
    main()  # pylint: disable=no-value-for-parameter
