"""Integration test metadata module."""
import json
import os
from unittest.mock import MagicMock
from unittest.mock import Mock, PropertyMock, call

import sys
# Voxel51 is initializing db connecting during the import phase of the module, we need to mock it to prevent connection error
sys.modules['fiftyone'] = MagicMock()

import metadata.consumer.main
import mongomock
import pytest
from pytest_mock import MockerFixture

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


@pytest.mark.integration
class TestMain:
    @pytest.fixture
    def boto3_mock(self, mocker: MockerFixture):
        mock = mocker.patch('metadata.consumer.main.boto3')
        return mock

    @pytest.fixture
    def container_services_mock(self, mocker: MockerFixture) -> Mock:
        container_services_mock = mocker.patch(
            'metadata.consumer.main.ContainerServices', autospec=True)
        db_tables_mock = PropertyMock(return_value={
            'recordings': 'recordings',
            'signals': 'signals',
            'pipeline_exec': 'pipeline_exec',
            'algo_output': 'algo_output'
        })
        type(container_services_mock.return_value).db_tables = db_tables_mock
        return container_services_mock

    @pytest.fixture(autouse=True)
    def graceful_exit_mock(self, mocker: MockerFixture) -> Mock:
        graceful_exit_mock = mocker.patch('metadata.consumer.main.GracefulExit')
        continue_running_mock = PropertyMock(
            side_effect=[True, True, True, False])
        type(graceful_exit_mock.return_value).continue_running = continue_running_mock
        return continue_running_mock

    @pytest.fixture
    def mongomock_fix(self):
        mockdb_client = mongomock.MongoClient()
        _ = mockdb_client.DataIngestion['pipeline_exec']
        _ = mockdb_client.DataIngestion['algo_output']
        _ = mockdb_client.DataIngestion['recordings']
        _ = mockdb_client.DataIngestion['signals']
        return mockdb_client.DataIngestion

    @pytest.fixture
    def message_attributes(self) -> dict:
        return {
            "SourceContainer": {"StringValue": "SDRetriever", "DataType": "String"},
            "ToQueue": {
                "StringValue": "metadata-queue",
                "DataType": "String"
            }
        }

    @pytest.fixture
    def input_message_recording(self, message_attributes: dict) -> dict:
        body = {
            "_id": "ridecare_device_recording_1662080172308_1662080561893",
            "MDF_available": "Yes",
            "media_type": "video",
            "s3_path": "bucket/folder/ridecare_device_recording_1662080172308_1662080561893.mp4",
            "footagefrom": 1662080172308,
            "footageto": 1662080561893,
            "tenant": "ridecare",
            "deviceid": "device",
            "length": "0:06:31",
            "sync_file_ext": "_metadata_full.json",
            "resolution": "640x360"
        }

        message = {
            'Body': json.dumps(body).replace('\"', '\''),
            'MessageAttributes': message_attributes,
            'ReceiptHandle': 'receipt_handle'
        }

        return message

    @pytest.fixture
    def input_message_snapshot_body_template(self) -> dict:
        return {
            "MDF_available": "Yes",
            "media_type": "image",
            "tenant": "ridecare",
            "deviceid": "device",
            "resolution": "640x360"
        }

    @pytest.fixture
    def input_message_snapshot_included(self, input_message_snapshot_body_template: dict, message_attributes: dict):
        input_message_snapshot_body_template["_id"] = "ridecare_device_snapshot_1662080178308"
        input_message_snapshot_body_template["s3_path"] = "bucket/folder/ridecare_snapshot_1662080178308.jpeg"
        input_message_snapshot_body_template["timestamp"] = 1662080178308

        message = {
            'Body': json.dumps(input_message_snapshot_body_template).replace('\"', '\''),
            'MessageAttributes': message_attributes,
            'ReceiptHandle': 'receipt_handle'
        }

        return message

    @pytest.fixture
    def input_message_snapshot_excluded(self, input_message_snapshot_body_template: dict, message_attributes: dict):
        input_message_snapshot_body_template["_id"] = "ridecare_device_snapshot_1692080178308"
        input_message_snapshot_body_template["s3_path"] = "bucket/folder/ridecare_snapshot_1692080178308.jpeg"
        input_message_snapshot_body_template["timestamp"] = 1692080178308

        message = {
            'Body': json.dumps(input_message_snapshot_body_template).replace('\"', '\''),
            'MessageAttributes': message_attributes,
            'ReceiptHandle': 'receipt_handle'
        }

        return message

    @pytest.fixture
    def environ_mock(self, mocker: MockerFixture) -> Mock:
        environ_mock = mocker.patch.dict(
            'metadata.consumer.main.os.environ', {'ANON_S3': 'anon_bucket'})
        return environ_mock


    @pytest.fixture(autouse=True)
    def voxel_mock(self, mocker: MockerFixture) -> tuple[Mock, Mock]:
        create_dataset_mock = mocker.patch('metadata.consumer.main.create_dataset')
        update_sample_mock = mocker.patch('metadata.consumer.main.update_sample')
        return create_dataset_mock, update_sample_mock


    @pytest.mark.integration
    def test_snapshot_video_correlation(self, environ_mock: Mock, container_services_mock: Mock,
                                        mongomock_fix: Mock, input_message_recording, input_message_snapshot_included,
                                        input_message_snapshot_excluded, voxel_mock: tuple[Mock, Mock]):
        # GIVEN
        container_services_mock.return_value.create_db_client.return_value = mongomock_fix
        container_services_mock.return_value.listen_to_input_queue.side_effect = [
            input_message_snapshot_included,
            input_message_snapshot_excluded,
            input_message_recording
        ]

        # WHEN
        metadata.consumer.main.main()

        # THEN
        snapshot_included_db_entry = mongomock_fix['recordings'].find_one(
            {'video_id': 'ridecare_device_snapshot_1662080178308'})
        snapshot_excluded_db_entry = mongomock_fix['recordings'].find_one(
            {'video_id': 'ridecare_device_snapshot_1692080178308'})
        recording_db_entry = mongomock_fix['recordings'].find_one(
            {'video_id': 'ridecare_device_recording_1662080172308_1662080561893'})
        # assertions on included snapshot
        # print(snapshot_included_db_entry)
        assert (snapshot_included_db_entry['recording_overview']
                ['source_videos'][0] == recording_db_entry['video_id'])

        # assertions on excluded snapshot
        assert (
            snapshot_excluded_db_entry['recording_overview']['source_videos'] == [])

        # assertions on recording
        assert (recording_db_entry['recording_overview']['#snapshots'] == 1)
        assert (
            len(recording_db_entry['recording_overview']['snapshots_paths']) == 1)
        assert (recording_db_entry['recording_overview']['snapshots_paths']
                [0] == snapshot_included_db_entry['video_id'])

        # assertions for voxel code
        create_dataset, update_sample = voxel_mock
        create_dataset.assert_has_calls(
            [call('folder'), call('folder_snapshots')], any_order=True)

        snapshot_excluded_db_entry.pop('_id')
        snapshot_excluded_db_entry['s3_path'] = f"s3://{environ_mock['ANON_S3']}/folder/ridecare_snapshot_1692080178308_anonymized.jpeg"
        recording_db_entry.pop('_id')
        recording_db_entry['s3_path'] = f"s3://{environ_mock['ANON_S3']}/folder/ridecare_device_recording_1662080172308_1662080561893_anonymized.mp4"
        snapshot_included_db_entry.pop('_id')
        snapshot_included_db_entry['s3_path'] = f"s3://{environ_mock['ANON_S3']}/folder/ridecare_snapshot_1662080178308_anonymized.jpeg"
        print(update_sample.call_count)
        update_sample.assert_has_calls([
            call('folder_snapshots', snapshot_excluded_db_entry),
            call('folder', recording_db_entry),

            # this verification will currently fail until the source_video field is correctly updated in voxel
            call('folder_snapshots', snapshot_included_db_entry)
        ], any_order=True)
