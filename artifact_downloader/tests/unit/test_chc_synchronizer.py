# pylint: disable=missing-function-docstring,missing-module-docstring
import json
from typing import Union
from datetime import timedelta
from base.testing.utils import load_relative_json_file, load_relative_raw_file

import pytest
from moto import mock_s3
from artifact_downloader.chc_synchronizer import ChcSynchronizer


@pytest.fixture(name="chc_s3_info")
def fixture_chc_s3_info():
    return "dev-rcd-anonymized-video-files", "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1678699387000_1678699429518_chc.json"  # pylint: disable=line-too-long


@pytest.fixture(name="chc_raw_file")
def fixture_chc_raw_file():
    return load_relative_raw_file(
        __file__, "data/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1678699387000_1678699429518_chc.json")


@pytest.fixture(name="chc_syncronized_file")
def fixture_chc_syncronized_file():
    return load_relative_json_file(
        __file__, "data/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1678699387000_1678699429518_signals.json")


@pytest.fixture(name="chc_synchronizer")
def fixture_chc_synchronizer(chc_raw_file, chc_s3_info):
    bucket, key = chc_s3_info

    with mock_s3():
        synchronizer = ChcSynchronizer("us-east-1")
        # pylint: disable=protected-access
        synchronizer._ChcSynchronizer__s3_client.create_bucket(Bucket=bucket)
        synchronizer._ChcSynchronizer__s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=chc_raw_file)
        yield synchronizer


@pytest.mark.unit
def test_download(chc_synchronizer: ChcSynchronizer, chc_raw_file, chc_s3_info):
    # GIVEN
    bucket, key = chc_s3_info
    file_data = json.loads(chc_raw_file)

    # WHEN
    result = chc_synchronizer.download(bucket, key)

    # THEN
    assert result == file_data


@pytest.mark.unit
def test_synchronize(chc_synchronizer: ChcSynchronizer, chc_raw_file,
                     chc_syncronized_file: dict[str, Union[bool, float, int]]):

    # GIVEN
    video_length = timedelta(seconds=33)
    json_data = json.loads(chc_raw_file)

    # WHEN
    result = chc_synchronizer.synchronize(json_data, video_length)
    result_json = {str(td): data for td, data in result.items()}

    # THEN
    assert chc_syncronized_file == result_json
