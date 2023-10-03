""" Downloader main module """
import logging

from kink import inject

from artifact_downloader.bootstrap import bootstrap_di
from artifact_downloader.handler import Handler

_logger = logging.getLogger(__name__)


@inject
def main(handler: Handler):
    """ downloader main entry point """
    _logger.info("Starting artifact_downloader...")
    handler.run()


if __name__ == "__main__":
    bootstrap_di()
    main()  # type: ignore[call-arg] # pylint: disable=no-value-for-parameter
