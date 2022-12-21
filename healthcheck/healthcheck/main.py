# type: ignore
"""healthcheck main module."""
import logging

from kink import inject

from healthcheck.bootstrap import bootstrap_di
from healthcheck.constants import CONTAINER_NAME
from healthcheck.worker import HealthCheckWorker


@inject
def start(worker: HealthCheckWorker, container_version: str, logger: logging.Logger):
    """Start."""
    logger.info("Starting Container %s:%s...", CONTAINER_NAME, container_version)
    worker.run()


def main() -> None:
    """Main method."""
    start()  # pylint: disable=no-value-for-parameter


if __name__ == "__main__":
    bootstrap_di()
    main()
