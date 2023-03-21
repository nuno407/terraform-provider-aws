""" Test the synchronizer module. """
import json
import os
from datetime import timedelta

import pytimeparse
from mdfparser.synchronizer import InvalidMdfException, Synchronizer
from pytest import fixture, mark, raises

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


@mark.unit
class TestSynchronizer:
    """ Test the Synchronizer class. """
    @fixture
    def mdf_data(self) -> dict:
        """ Load the test data from the test_assets folder. """
        with open(os.path.join(__location__,
                               "./test_assets/test_data/mdf_synthetic.json"),
                               "r",
                               encoding="utf-8") as f_handler:
            return json.loads(f_handler.read())

    @fixture
    def sync_data_expected(self) -> dict:
        """ Load the expected test data from the test_assets folder. """
        with open(os.path.join(__location__,
                               "./test_assets/test_data/sync_expected.json"),
                               "r",
                               encoding="utf-8") as f_handler:
            raw_json = json.loads(f_handler.read())
            return {timedelta(seconds=pytimeparse.parse(k)): v for k, v in raw_json.items()}

    @fixture
    def synchronizer(self) -> Synchronizer:
        """ Create a new Synchronizer instance. """
        return Synchronizer()

    def test_synchronize(self, synchronizer: Synchronizer, mdf_data: dict,
                         sync_data_expected: dict):
        """ Test the synchronize method. """
        # WHEN
        synchronized_data = synchronizer.synchronize(mdf_data, 1659962816, 1659962817)

        # THEN
        assert synchronized_data == sync_data_expected

    def test_synchronize_fails_for_incomplete_mdf(self, synchronizer: Synchronizer,
                                                  mdf_data: dict):
        """ Test the synchronize method. """
        # GIVEN
        del mdf_data["chunk"]["utc_end"]

        # WHEN
        with raises(InvalidMdfException):
            synchronizer.synchronize(mdf_data, 1659962816, 1659962817)
