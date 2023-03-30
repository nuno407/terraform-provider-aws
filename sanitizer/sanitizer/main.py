# type: ignore
""" main module. """
from kink import inject

from sanitizer.bootstrap import bootstrap_di
from sanitizer.handler import Handler


@inject
def main(handler: Handler):
    """ main function. """
    handler.run()


if __name__ == "__main__":
    bootstrap_di()
    main()  # pylint: disable=no-value-for-parameter
