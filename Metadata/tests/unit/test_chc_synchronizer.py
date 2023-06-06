# pylint: disable=missing-function-docstring,missing-module-docstring
import json
import os
import pickle
from datetime import timedelta

import pytest
import pytimeparse
from moto import mock_s3
from metadata.consumer.chc_synchronizer import ChcSynchronizer

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


@pytest.fixture(name="chc_s3_info")
def fixture_chc_s3_info():
    return "dev-rcd-anonymized-video-files", "Debug_Lync/honeybadger_rc_srx_develop_cst2hi_01_InteriorRecorder_1669280184000_1669280217441_chc.json"  # pylint: disable=line-too-long


@pytest.fixture(name="chc_raw_file")
def fixture_chc_raw_file():
    with open(os.path.join(__location__, "test_data/ivs_chc_raw_file.json"), "r") as f:
        return f.read()


@pytest.fixture(name="chc_syncronized_file")
def fixture_chc_syncronized_file():
    with open(os.path.join(__location__, "test_data/chc_synchronized.pickle"), "rb") as f:
        return pickle.load(f)


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
def test_download(chc_synchronizer, chc_raw_file, chc_s3_info):
    # GIVEN
    bucket, key = chc_s3_info
    file_data = json.loads(chc_raw_file)

    # WHEN
    result = chc_synchronizer.download(bucket, key)

    # THEN
    assert result == file_data


@pytest.mark.unit
def test_synchronize(chc_synchronizer, chc_raw_file, chc_syncronized_file):
    # GIVEN
    video_length = timedelta(seconds=pytimeparse.parse("0:00:33"))
    json_data = json.loads(chc_raw_file)

    # WHEN
    result = chc_synchronizer.synchronize(json_data, video_length)

    # THEN
    assert chc_syncronized_file == result