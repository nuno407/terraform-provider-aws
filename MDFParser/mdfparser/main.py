""" main module """
from logging import Logger, getLogger

from kink import inject
from mdfparser.bootstrap import bootstrap_di  # type: ignore
from mdfparser.consumer import Consumer

CONTAINER_NAME = "MDFParser"  # Name of the current container
CONTAINER_VERSION = "v1.0"  # Version of the current container

_logger: Logger = getLogger("mdfparser." + __name__)


@inject
def main(consumer: Consumer, container_name: str, container_version: str) -> None:  # pylint: disable=too-many-locals,too-many-statements
    """Main function of the MDFParser container."""
    _logger.info("Starting Container %s (%s)..\n", container_name, container_version)
    consumer.run()


def init() -> None:
    """
    Initilizes the dependency injection.
    """
    bootstrap_di()


if __name__ == "__main__":
    init()
    main()  # type: ignore # pylint: disable=no-value-for-parameter
