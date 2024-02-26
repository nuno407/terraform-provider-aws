# pylint: disable=missing-function-docstring,missing-module-docstring,missing-class-docstring
import json
import os
from datetime import datetime, timezone
import pytz

from fiftyone import ViewField

from base.testing.utils import get_abs_path
from base.voxel.functions import create_dataset
from base.model.config.dataset_config import DatasetConfig
from unittest.mock import Mock, PropertyMock, patch, ANY, call
import fiftyone as fo

import mongomock
import pytest
from pytest_mock import MockerFixture
from kink import di
import metadata.consumer.main

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def _message_attributes() -> dict:
    return {
        "SourceContainer": {"StringValue": "SDRetriever", "DataType": "String"},
        "ToQueue": {
            "StringValue": "metadata-queue",
            "DataType": "String"
        }
    }


def _pack_message(body: dict) -> dict:
    message = {
        "Body": json.dumps(body),
        "MessageAttributes": _message_attributes(),
        "ReceiptHandle": "receipt_handle"
    }

    # read_message function fix some issues on message structure
    return metadata.consumer.main.fix_message(Mock(), json.dumps(message), message)


def _input_message_recording(folder) -> dict:
    body = {
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_recording_1662080172308_1662080561893.mp4",
        "timestamp": 1662080172308,
        "end_timestamp": 1662080561893,
        "artifact_name": "s3_video",
        "artifact_id": "device_InteriorRecorder_ridecare_device_recording_1662080172308_1662080561893",
        "raw_s3_path": "s3://raw/foo/bar.something",
        "anonymized_s3_path": "s3://anonymized/foo/bar.something",
        "tenant_id": "ridecare",
        "device_id": "device",
        "actual_duration": 391.0,
        "recorder": "InteriorRecorder",
        "resolution": {"width": 640, "height": 360},
        "upload_timing": {"start": 1662080561993, "end": 1662080562093},
        "recordings": [{"chunk_ids": [1, 2, 3], "recording_id": "some_recording_id_1"}],
        "footage_id": "ridecare_device_recording",
        "rcc_s3_path": "s3://dev-rcc-video-repo/datanauts/d76b5b32-f543-47cf-a350-7d3bcb7144f0/TRAINING/Footage_89983c99-8ff5-4eb1-9140-ca019e70c1c0.mp4"
    }

    message = _pack_message(body)

    return message


def _input_message_snapshot(folder: str, timestamp: int) -> dict:
    body = {
        "artifact_name": "snapshot",
        "artifact_id": "ridecare_device_foo_1612080178308",
        "raw_s3_path": "s3://raw/foo/bar.something",
        "anonymized_s3_path": "s3://anonymized/foo/bar.something",
        "tenant_id": "ridecare",
        "device_id": "device",
        "resolution": {"width": 640, "height": 360},
        "timestamp": timestamp,
        "end_timestamp": timestamp,
        "upload_timing": {"start": timestamp + 1000, "end": timestamp + 2000},
        "recorder": "TrainingMultiSnapshot",
        "uuid": "foo",
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_foo_{timestamp}.jpeg",
    }
    return _pack_message(body)


def _input_message_snapshot_included(folder: str):
    body = {
        "artifact_name": "snapshot",
        "artifact_id": "ridecare_device_foo_1662080178308",
        "raw_s3_path": "s3://raw/foo/bar.something",
        "anonymized_s3_path": "s3://anonymized/foo/bar.something",
        "tenant_id": "ridecare",
        "device_id": "device",
        "resolution": {"width": 640, "height": 360},
        "timestamp": 1662080178308,
        "end_timestamp": 1662080178308,
        "upload_timing": {"start": 1662080178308 + 1000, "end": 1662080178308 + 2000},
        "recorder": "TrainingMultiSnapshot",
        "uuid": "foo",
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_foo_1662080178308.jpeg",
    }
    return _pack_message(body)


def _input_message_snapshot_excluded(folder: str):
    body = {
        "artifact_name": "snapshot",
        "artifact_id": "ridecare_device_foo_1612080178308",
        "raw_s3_path": "s3://raw/foo/bar.something",
        "anonymized_s3_path": "s3://anonymized/foo/bar.something",
        "tenant_id": "ridecare",
        "device_id": "device",
        "resolution": {"width": 640, "height": 360},
        "timestamp": 1612080178308,
        "end_timestamp": 1612080178308,
        "upload_timing": {"start": 1612080178308 + 1000, "end": 1612080178308 + 2000},
        "recorder": "TrainingMultiSnapshot",
        "uuid": "foo",
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_foo_1612080178308.jpeg",
    }
    message = _pack_message(body)
    return message


@pytest.mark.integration
@patch.dict("metadata.consumer.main.os.environ",
            {"TENANT_MAPPING_CONFIG_PATH": "./config/config.yml"})
@patch.dict("metadata.consumer.main.os.environ", {"MONGODB_CONFIG": "./config/mongo_config.yml"})
class TestMain:
    @pytest.fixture
    def boto3_mock(self, mocker: MockerFixture):
        mock = mocker.patch("metadata.consumer.main.boto3")
        return mock

    @pytest.fixture
    def container_services_mock(self, mocker: MockerFixture) -> Mock:
        container_services_mock = mocker.patch(
            "metadata.consumer.main.ContainerServices", autospec=True)
        db_tables_mock = PropertyMock(return_value={
            "recordings": "recordings",
            "signals": "signals",
            "pipeline_exec": "pipeline_exec",
            "algo_output": "algo_output",
            "processed_imu": "processed_imu",
            "events": "events"
        })
        type(container_services_mock.return_value).db_tables = db_tables_mock
        return container_services_mock

    @pytest.fixture(autouse=True)
    def graceful_exit_mock(self, mocker: MockerFixture) -> Mock:
        graceful_exit_mock = mocker.patch("metadata.consumer.main.GracefulExit")
        continue_running_mock = PropertyMock(
            side_effect=[True, True, True, False])
        type(graceful_exit_mock.return_value).continue_running = continue_running_mock
        return continue_running_mock

    @pytest.fixture
    def mongomock_fix(self):
        mockdb_client = mongomock.MongoClient(tz_aware=True)
        _ = mockdb_client.DataIngestion["pipeline_exec"]
        _ = mockdb_client.DataIngestion["algo_output"]
        _ = mockdb_client.DataIngestion["recordings"]
        _ = mockdb_client.DataIngestion["signals"]
        return mockdb_client.DataIngestion

    @pytest.fixture
    def environ_mock(self, mocker: MockerFixture) -> Mock:
        environ_mock = mocker.patch.dict(
            "metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket"})
        return environ_mock

    @pytest.mark.integration
    @pytest.mark.parametrize("_input_message_recording, _input_message_snapshot_included, "
                             "_input_message_snapshot_excluded, s3_folder, expected_dataset", [
                                 (_input_message_recording("folder"),
                                     _input_message_snapshot_included("folder"),
                                     _input_message_snapshot_excluded("folder"),
                                     "folder",
                                     "Debug_Lync"),
                                 (_input_message_recording("ridecare_companion_gridwise"),
                                     _input_message_snapshot_included(
                                         "ridecare_companion_gridwise"),
                                     _input_message_snapshot_excluded(
                                         "ridecare_companion_gridwise"),
                                     "ridecare_companion_gridwise",
                                     "RC-ridecare_companion_gridwise")
                             ])
    @patch("metadata.consumer.main.connect")
    @patch.dict("metadata.consumer.main.os.environ",
                {"TENANT_MAPPING_CONFIG_PATH": get_abs_path(__file__, "test_data/config.yml")})
    @patch.dict("metadata.consumer.main.os.environ",
                {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
    @patch.dict("metadata.consumer.main.os.environ", {"FIFTYONE_DATABASE_URI": "db_uri"})
    def test_snapshot_video_correlation(self, _: Mock, environ_mock: Mock, container_services_mock: Mock,  # pylint: disable=too-many-arguments,redefined-outer-name
                                        mongomock_fix: Mock, _input_message_recording, _input_message_snapshot_included,
                                        _input_message_snapshot_excluded, s3_folder, expected_dataset):
        # GIVEN
        container_services_mock.return_value.create_db_client.return_value = mongomock_fix
        container_services_mock.return_value.anonymized_s3 = "anon_bucket"
        container_services_mock.return_value.raw_s3 = "raw_bucket"
        container_services_mock.return_value.get_single_message_from_input_queue.side_effect = [
            _input_message_snapshot_included,
            _input_message_snapshot_excluded,
            _input_message_recording
        ]

        # WHEN
        metadata.consumer.main.main()

        # THEN
        snapshot_included_db_entry = mongomock_fix["recordings"].find_one(
            {"video_id": "ridecare_device_foo_1662080178308"})
        snapshot_excluded_db_entry = mongomock_fix["recordings"].find_one(
            {"video_id": "ridecare_device_foo_1612080178308"})
        recording_db_entry = mongomock_fix["recordings"].find_one(
            {"video_id": "device_InteriorRecorder_ridecare_device_recording_1662080172308_1662080561893"})
        # assertions on included snapshot
        assert (snapshot_included_db_entry["recording_overview"]
                ["source_videos"][0] == recording_db_entry["video_id"])

        # assert reference id is present
        assert snapshot_included_db_entry["recording_overview"]["devcloudid"]
        assert snapshot_excluded_db_entry["recording_overview"]["devcloudid"]
        assert recording_db_entry["recording_overview"]["devcloudid"]

        # assert recording time is present
        assert (snapshot_included_db_entry["recording_overview"]["recording_time"]
                == datetime(2022, 9, 2, 0, 56, 18, 308000, tzinfo=timezone.utc))
        assert (snapshot_excluded_db_entry["recording_overview"]["recording_time"]
                == datetime(2021, 1, 31, 8, 2, 58, 308000, tzinfo=timezone.utc))

        assert recording_db_entry["recording_overview"]["time"] == "2022-09-02 00:56:12"
        assert (recording_db_entry["recording_overview"]["recording_time"]
                == datetime(2022, 9, 2, 0, 56, 12, 308000, tzinfo=timezone.utc))

        # assertions on excluded snapshot
        assert snapshot_excluded_db_entry["recording_overview"]["source_videos"] == []

        # assertions on recording
        assert recording_db_entry["recording_overview"]["#snapshots"] == 1
        assert len(recording_db_entry["recording_overview"]["snapshots_paths"]) == 1
        assert recording_db_entry["recording_overview"]["snapshots_paths"][0] == snapshot_included_db_entry["video_id"]

        # assertions for voxel code
        dataset = create_dataset(expected_dataset, ["RC"])
        assert dataset.tags == ["RC"]

        ds_snapshots = create_dataset(expected_dataset + "_snapshots", ["RC"])
        assert ds_snapshots.tags == ["RC"]

        sample_excluded = get_sample(
            ds_snapshots,
            f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_foo_1612080178308_anonymized.jpeg")
        assert sample_excluded["raw_filepath"] == snapshot_excluded_db_entry["filepath"]

        sample_recording = get_sample(
            dataset,
            f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_recording_1662080172308_1662080561893_anonymized.mp4")
        assert sample_recording["raw_filepath"] == recording_db_entry["filepath"]

        sample_included = get_sample(
            ds_snapshots,
            f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_foo_1662080178308_anonymized.jpeg")
        assert sample_included["raw_filepath"] == snapshot_included_db_entry["filepath"]

    @patch("metadata.consumer.main.connect")
    @patch.dict("metadata.consumer.main.os.environ",
                {"TENANT_MAPPING_CONFIG_PATH": get_abs_path(__file__, "test_data/config.yml")})
    @patch.dict("metadata.consumer.main.os.environ",
                {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
    @patch.dict("metadata.consumer.main.os.environ", {"FIFTYONE_DATABASE_URI": "db_uri"})
    def test_main_no_message_attributes(
            self, _: Mock, container_services_mock: Mock, mongomock_fix: Mock):
        # GIVEN
        message = {
            "no": "message_attributes",
            "ReceiptHandle": "receipt_handle"
        }
        container_services_mock.return_value.create_db_client.return_value = mongomock_fix
        container_services_mock.return_value.anonymized_s3 = "anon_bucket"
        container_services_mock.return_value.raw_s3 = "raw_bucket"
        container_services_mock.return_value.get_single_message_from_input_queue.side_effect = [
            message] * 3

        # WHEN
        metadata.consumer.main.main()

        # THEN
        calls = [call(ANY, "receipt_handle")] * 3
        container_services_mock.return_value.delete_message.assert_has_calls(calls)

    @pytest.mark.integration
    def test_transform_data_to_update_query(self):
        # GIVEN
        input_data = {
            "a": 1,
            "b": {"c": True},
            "d": {"e": {"f": [1, 2, 3]}},
            "g": {"h": None}
        }

        # WHEN
        result = metadata.consumer.main.transform_data_to_update_query(
            input_data)

        # THEN
        assert result == {
            "a": 1,
            "b.c": True,
            "d.e.f": [1, 2, 3]
        }


def get_sample(dataset, s3_path: str):
    return dataset.one(ViewField("s3_path") == s3_path)


@pytest.mark.integration
@pytest.mark.skip(reason="This test is only passing sometimes in the pipeline")
def test_update_rule_on_voxel():
    # GIVEN
    dataset_config = DatasetConfig(default_dataset="RC-datanauts", tag="RC")
    di[DatasetConfig] = dataset_config
    raw_filepath = "s3://qa-rcd-raw-video-files/datanauts/srxfut2internal01_rc_srx_qa_eur_fut2_013_InteriorRecorder_1643259764422_1643261551249.mp4"
    video_id = "video_id"
    tenant = "datanauts"
    rules = {"name": "artifact.rule.rule_name",
             "version": "artifact.rule.rule_version",
             "origin": "artifact.rule.origin",
             "footage_from": datetime.strptime("2023-12-07 16:09:29", "%Y-%m-%d %H:%M:%S"),
             "footage_to": datetime.strptime("2023-12-07 16:11:45", "%Y-%m-%d %H:%M:%S")}
    # WHEN
    metadata.consumer.main.update_rule_on_voxel(raw_filepath, video_id, tenant, rules)
    # Exact same rule called twice should not add a new element
    metadata.consumer.main.update_rule_on_voxel(raw_filepath, video_id, tenant, rules)
    # THEN
    dataset = fo.load_dataset("RC-datanauts")
    sample = dataset.one(ViewField("video_id") == video_id)
    assert len(sample["rules"]) == 1
    assert sample["rules"][0].to_dict() == fo.DynamicEmbeddedDocument(**rules).to_dict()
