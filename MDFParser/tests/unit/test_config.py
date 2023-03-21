""" Unit tests for the MdfParserConfig class. """
import os

import pytest

from mdfparser.config import MdfParserConfig

__location__ = os.path.realpath(os.path.join(
    os.getcwd(), os.path.dirname(__file__)))


@pytest.mark.unit
class TestMdfParserConfig:
    """ Test the MdfParserConfig class. """

    def test_current_config(self):
        """ Test that the config loader works. """
        # GIVEN
        good_config = os.path.join(
            __location__, "./test_assets/MdfParserConfigs/config.yml")

        # WHEN
        loaded_config = MdfParserConfig.load_config_from_yaml_file(good_config)

        # THEN
        assert len(loaded_config.input_queue) != 0
        assert len(loaded_config.metadata_output_queue) != 0

    def test_good_config(self):
        """ Test that the config loader works. """
        # GIVEN
        good_config = os.path.join(
            __location__, "./test_assets/MdfParserConfigs/config_good.yml")

        # WHEN
        loaded_config = MdfParserConfig.load_config_from_yaml_file(good_config)

        # THEN
        assert loaded_config.input_queue == "dev-terraform-queue-mdf-parser"
        assert loaded_config.metadata_output_queue == "dev-terraform-queue-metadata"

    def test_bad_config_extra_keys(self):
        """ Test that the config loader fails when an extra key is present. """
        # GIVEN
        bad_config = os.path.join(
            __location__, "./test_assets/MdfParserConfigs/config_extra_keys.yml")

        # WHEN / THEN
        assert MdfParserConfig.load_config_from_yaml_file(
            bad_config) is not None

    def test_bad_config_missing_keys(self):
        """ Test that the config loader fails when a required key is missing. """
        # GIVEN
        bad_config = os.path.join(
            __location__, "./test_assets/MdfParserConfigs/config_missing_keys.yml")

        # WHEN / THEN
        with pytest.raises(Exception):
            MdfParserConfig.load_config_from_yaml_file(bad_config)
