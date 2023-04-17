# pylint: disable=missing-function-docstring,missing-module-docstring,missing-class-docstring
import json
import os
from unittest.mock import Mock, PropertyMock, call, patch

import mongomock
import pytest
from pytest_mock import MockerFixture

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
    return metadata.consumer.main.read_message(Mock(), json.dumps(message))


def _input_message_recording(folder) -> dict:
    body = {
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_recording_1662080172308_1662080561893.mp4",
        "timestamp": 1662080172308,
        "end_timestamp": 1662080561893,
        "actual_timestamp": 1662080172308,
        "actual_end_timestamp": 1662080561893,
        "tenant_id": "ridecare",
        "device_id": "device",
        "stream_name": "ridecare_device_recording",
        "actual_duration": 391.0,
        "recorder": "InteriorRecorder",
        "resolution": {"width": 640, "height": 360},
        "upload_timing": {"start": 1662080561993, "end": 1662080562093},
    }

    message = _pack_message(body)

    return message


def _input_message_snapshot(folder: str, timestamp: int) -> dict:
    body = {
        "tenant_id": "ridecare",
        "device_id": "device",
        "resolution": {"width": 640, "height": 360},
        "timestamp": timestamp,
        "upload_timing": {"start": timestamp + 1000, "end": timestamp + 2000},
        "recorder": "TrainingMultiSnapshot",
        "uuid": "foo",
        "s3_path": f"s3://bucket-raw-video-files/{folder}/ridecare_device_foo_{timestamp}.jpeg",
    }
    message = _pack_message(body)
    return message


def _input_message_snapshot_included(folder: str):
    return _input_message_snapshot(folder, 1662080178308)


def _input_message_snapshot_excluded(folder: str):
    return _input_message_snapshot(folder, 1612080178308)


@pytest.mark.integration
@patch.dict("metadata.consumer.main.os.environ", {"TENANT_MAPPING_CONFIG_PATH": "./config/config.yml"})
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
            "processed_imu": "processed_imu"
        })
        type(container_services_mock.return_value).db_tables = db_tables_mock
        return container_services_mock

    @pytest.fixture(autouse=True)
    def graceful_exit_mock(self, mocker: MockerFixture) -> Mock:
        graceful_exit_mock = mocker.patch(
            "metadata.consumer.main.GracefulExit")
        continue_running_mock = PropertyMock(
            side_effect=[True, True, True, False])
        type(graceful_exit_mock.return_value).continue_running = continue_running_mock
        return continue_running_mock

    @pytest.fixture
    def mongomock_fix(self):
        mockdb_client = mongomock.MongoClient()
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

    @pytest.fixture(autouse=True)
    def voxel_mock(self, mocker: MockerFixture) -> tuple[Mock, Mock]:
        create_dataset_mock = mocker.patch(
            "metadata.consumer.voxel.functions.create_dataset")
        update_sample_mock = mocker.patch(
            "metadata.consumer.voxel.functions.update_sample")
        return create_dataset_mock, update_sample_mock

    @pytest.mark.integration
    @pytest.mark.parametrize("_input_message_recording, _input_message_snapshot_included, "
                             "_input_message_snapshot_excluded, s3_folder, expected_dataset", [
                                 (_input_message_recording("folder"),
                                     _input_message_snapshot_included(
                                         "folder"),
                                     _input_message_snapshot_excluded(
                                         "folder"),
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
    def test_snapshot_video_correlation(self, environ_mock: Mock, container_services_mock: Mock,  # pylint: disable=too-many-arguments,redefined-outer-name
                                        mongomock_fix: Mock, _input_message_recording, _input_message_snapshot_included,
                                        _input_message_snapshot_excluded, s3_folder, expected_dataset,
                                        voxel_mock: tuple[Mock, Mock]):
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
            {"video_id": "ridecare_device_recording_1662080172308_1662080561893"})
        # assertions on included snapshot
        assert (snapshot_included_db_entry["recording_overview"]
                ["source_videos"][0] == recording_db_entry["video_id"])

        # assert reference id is present
        assert snapshot_included_db_entry["recording_overview"]["devcloudid"]
        assert snapshot_excluded_db_entry["recording_overview"]["devcloudid"]
        assert recording_db_entry["recording_overview"]["devcloudid"]

        # assertions on excluded snapshot
        assert snapshot_excluded_db_entry["recording_overview"]["source_videos"] == [
        ]

        # assertions on recording
        assert recording_db_entry["recording_overview"]["#snapshots"] == 1
        assert len(
            recording_db_entry["recording_overview"]["snapshots_paths"]) == 1
        assert recording_db_entry["recording_overview"]["snapshots_paths"][
            0] == snapshot_included_db_entry["video_id"]  # pylint: disable=line-too-long

        # assertions for voxel code
        create_dataset, update_sample = voxel_mock
        create_dataset.assert_has_calls(
            [call(expected_dataset, ["RC"]), call(expected_dataset + "_snapshots", ["RC"])], any_order=True)

        snapshot_excluded_db_entry.pop("_id")
        snapshot_excluded_db_entry["s3_path"] = f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_foo_1612080178308_anonymized.jpeg"  # pylint: disable=line-too-long
        snapshot_excluded_db_entry["raw_filepath"] = snapshot_excluded_db_entry["filepath"]
        recording_db_entry.pop("_id")
        recording_db_entry["s3_path"] = f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_recording_1662080172308_1662080561893_anonymized.mp4"  # pylint: disable=line-too-long
        recording_db_entry["raw_filepath"] = recording_db_entry["filepath"]
        snapshot_included_db_entry.pop("_id")
        snapshot_included_db_entry["s3_path"] = f"s3://{environ_mock['ANON_S3']}/{s3_folder}/ridecare_device_foo_1662080178308_anonymized.jpeg"  # pylint: disable=line-too-long
        snapshot_included_db_entry["raw_filepath"] = snapshot_included_db_entry["filepath"]

        # assert_has_calls does not work yet with pytest: https://github.com/pytest-dev/pytest-mock/issues/234
        update_sample.assert_has_calls([
            call(expected_dataset + "_snapshots", snapshot_excluded_db_entry),
            call(expected_dataset, recording_db_entry),
            call(expected_dataset + "_snapshots", snapshot_included_db_entry)
        ], any_order=True)

    @pytest.mark.unit
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
