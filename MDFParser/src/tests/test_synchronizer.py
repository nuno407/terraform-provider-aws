from datetime import timedelta
import json
import os
from pytest import fixture, raises
import pytimeparse
from mdfparser.synchronizer import Synchronizer, InvalidMdfException

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

class TestSynchronizer:
    @fixture
    def mdf_data(self) -> dict:
        with open(os.path.join(__location__, './test_assets/test_data/mdf_synthetic.json'), 'r') as f:
            return json.loads(f.read())

    @fixture
    def sync_data_expected(self) -> dict:
        with open(os.path.join(__location__, './test_assets/test_data/sync_expected.json'), 'r') as f:
            raw_json = json.loads(f.read())
            return {timedelta(seconds=pytimeparse.parse(k)): v for k, v in raw_json.items()}

    @fixture
    def synchronizer(self) -> Synchronizer:
        return Synchronizer()

    def test_synchronize(self, synchronizer: Synchronizer, mdf_data: dict, sync_data_expected: dict):
        # WHEN
        synchronized_data = synchronizer.synchronize(mdf_data, 1659962816, 1659962817)

        # THEN
        assert(synchronized_data == sync_data_expected)

    def test_synchronize_fails_for_incomplete_mdf(self, synchronizer: Synchronizer, mdf_data: dict):
        # GIVEN
        del mdf_data['chunk']['utc_end']

        # WHEN
        with raises(InvalidMdfException):
            synchronizer.synchronize(mdf_data, 1659962816, 1659962817)
