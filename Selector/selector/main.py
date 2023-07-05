"""Selector container script"""
# pylint: disable=no-value-for-parameter
from kink import inject

from base.aws.container_services import ContainerServices
from selector.bootstrap import bootstrap_di
from selector.constants import CONTAINER_NAME, CONTAINER_VERSION
from selector.selector import Selector

_logger = ContainerServices.configure_logging("selector")


@inject
def main(selector: Selector):
    """Main function"""

    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)
    selector.run()  # type: ignore


if __name__ == "__main__":
    bootstrap_di()
    main()  # type: ignore
