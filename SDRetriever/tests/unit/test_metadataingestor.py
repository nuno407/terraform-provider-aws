import json
import os
import pickle
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, MagicMock, call, patch

import pytest

from sdretriever.ingestor.metadata import MetadataIngestor


@pytest.mark.unit
@pytest.mark.usefixtures("msg_interior",
                         "container_services",
                         "s3_client",
                         "sqs_client",
                         "sts_helper",
                         "metadata_ingestor",
                         "metacontent_chunks_metadata",
                         "message_metadata")
class TestMetadataIngestor:

    @pytest.fixture
    def expected_rcc_folders(self):
        return [
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/',
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=17/',
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/'
        ]

    @pytest.fixture
    def source_data(self):
        return {
            "key1": "value1",
            "key2": "value2",
        }

    @pytest.fixture
    def metadata_ingestor(self, container_services, s3_client, sqs_client, sts_helper):
        return MetadataIngestor(container_services, s3_client, sqs_client, sts_helper)

    def test_json_raise_on_duplicates(self, metadata_ingestor):
        json_with_duplicates = '''{"chunk": {"pts_start": "25152337","pts_end": "26182733"},"chunk": {"utc_start": "1655453665526","utc_end": "1655453676953"}}'''
        result = json.loads(json_with_duplicates,
                            object_pairs_hook=metadata_ingestor._json_raise_on_duplicates)
        expected_result = dict(chunk={"pts_start": "25152337", "pts_end": "26182733",
                               "utc_start": "1655453665526", "utc_end": "1655453676953"})
        assert result == expected_result

    @Mock('gzip.decompress', side_effect=[b"{1:2,3:4}", b"{5:6,7:8}"])
    def test_get_metadata_chunks(self, msg_interior, metadata_ingestor, metadata_files):
        metadata_start_time = datetime.fromtimestamp(
            msg_interior.uploadstarted / 1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(
            msg_interior.uploadfinished / 1000.0).replace(microsecond=0, second=0, minute=0)
        files_to_download = [
            "datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/" + file for file in metadata_files]
        with open("artifacts/metadata_response.pickle", "rb") as f:
            response_dict = pickle.load(f)
        metadata_ingestor.check_if_exists = Mock(return_value=(True, response_dict))

        result = metadata_ingestor._get_metadata_chunks(
            metadata_start_time, metadata_end_time, msg_interior)

        metadata_ingestor.container_svcs.download_file.assert_has_calls(
            [call(ANY, metadata_ingestor.container_svcs.rcc_info["s3_bucket"], file_name) for file_name in files_to_download], any_order=True)
        metadata_ingestor.check_if_exists.assert_called_with(
            "datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/InteriorRecorder_InteriorRecorder-77d21ada-c79e-48c7-b582-cfc737773f26",
            "dev-rcc-raw-video-data")
        assert result == {0: {1: 2, 3: 4}, 1: {5: 6, 7: 8}}

    def test_process_chunks_into_mdf(self, metadata_ingestor, metadata_chunks, msg_interior):
        resolution, pts, mdf_data = metadata_ingestor._process_chunks_into_mdf(
            metadata_chunks, msg_interior)
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/ridecare_companion_fut_rc_srx_prod_a8c08d7b1c19f930892ba6b56fc885b7ffbd3275_InteriorRecorder_1677676914505_1677676967654_metadata_full.json", "r") as f:
            expected_metadata = json.load(f)

        assert resolution == expected_metadata["resolution"]
        assert mdf_data == expected_metadata["frame"]
        assert pts == expected_metadata["chunk"]

    def test_upload_source_data(self, metadata_ingestor, source_data, msg_interior):
        s3_path = metadata_ingestor._upload_source_data(
            source_data, msg_interior, "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110")
        expected_client = ANY
        expected_source_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False, indent=4).encode('UTF-8'))

        expected_path = "datanauts/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json"
        expected_bucket = metadata_ingestor.container_svcs.raw_s3
        metadata_ingestor.container_svcs.upload_file.assert_called_with(

            expected_client, expected_source_bytes, expected_bucket, expected_path)
        assert s3_path == expected_path

    def test_ingest(self, metadata_ingestor, message_metadata, metacontent_chunks_metadata, metadata_full):
        """ metadata_start_time = datetime.fromtimestamp(msg_interior.uploadstarted/1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(msg_interior.uploadfinished/1000.0).replace(microsecond=0, second=0, minute=0)"""

        mock_chunks_path = Mock()

        metadata_ingestor._get_metacontent_chunks = Mock(return_value=metacontent_chunks_metadata)
        metadata_ingestor._process_chunks_into_mdf = Mock(return_value=(
            metadata_full["resolution"],
            metadata_full["chunk"],
            metadata_full["frame"]
        ))
        metadata_ingestor._upload_source_data = Mock(return_value=(
            "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110",
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json"
        ))
        metadata_ingestor.container_svcs.send_message = Mock()
        result = metadata_ingestor.ingest(
            message_metadata,
            "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110",
            mock_chunks_path)

        metadata_ingestor._upload_source_data.assert_called_once_with(
            metadata_full, ANY, ANY)
        metadata_ingestor.container_svcs.send_message.assert_called_once_with(
            ANY, "dev-terraform-queue-mdf-parser", ANY)

        metadata_ingestor._get_metacontent_chunks.assert_called_once_with(ANY, mock_chunks_path)
        assert result

    def test_get_chunks_lookup_paths(self, metadata_ingestor, msg_interior):
        paths_reference = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/"
        ]

        expected_result = {
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_TYPE_TEST_ID"
        }
        metadata_ingestor._discover_s3_subfolders = Mock(
            return_value=paths_reference)

        start_time = datetime(year=2022, month=8, day=30, hour=20, minute=10)
        end_time = datetime(year=2022, month=11, day=30, hour=20)

        msg_interior.uploadstarted = start_time
        msg_interior.uploadfinished = end_time
        msg_interior.tenant = "TEST_TENANT"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"

        paths_to_test = set(metadata_ingestor._get_chunks_lookup_paths(msg_interior))

        assert paths_to_test == expected_result
        metadata_ingestor._discover_s3_subfolders.assert_called_once_with(
            "TEST_TENANT/TEST_DEVICE_ID/", ANY, ANY, start_time.replace(minute=0), end_time + timedelta(seconds=1))

    def test_get_chunks_lookup_paths2(self, metadata_ingestor, msg_interior):
        paths_reference = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=12/hour=21/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/"
        ]

        expected_result = {
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=12/hour=21/TEST_TYPE_TEST_ID",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_TYPE_TEST_ID"
        }
        metadata_ingestor._discover_s3_subfolders = Mock(
            return_value=paths_reference)

        start_time = datetime(year=2022, month=8, day=30, hour=20, minute=10)
        end_time = datetime(year=2022, month=11, day=30, hour=20)

        msg_interior.tenant = "TEST_TENANT"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"

        paths_to_test = set(metadata_ingestor._get_chunks_lookup_paths(
            msg_interior, start_time, end_time))

        assert paths_to_test == expected_result
        metadata_ingestor._discover_s3_subfolders.assert_called_once_with(
            "TEST_TENANT/TEST_DEVICE_ID/", ANY, ANY, start_time.replace(minute=0), end_time)

    def test_search_chunks_in_s3_path(self, metadata_ingestor):
        resp_mock = {
            'Contents': [
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_20.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4.something.zip',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-456bfdbg.mp4',
                 'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TrainingRecorder_TrainingRecorder-456bfdbg_10.mp4',
                 'LastModified': datetime(year=2022, month=10, day=10)
                 }
            ]
        }

        metadata_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4_stream.json.zip',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4.something.zip'}
        video_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_20.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_10.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_11.mp4'}
        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        metadata_ingestor.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

        metadata_chunks_set, video_chunks_set = metadata_ingestor._search_chunks_in_s3_path(
            reference_path, bucket, messageid, [".json", ".zip"], recorder_type="InteriorRecorder")

        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        metadata_ingestor.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    def test_search_chunks_in_s3_path_time_bound(self, metadata_ingestor):
        resp_mock = {
            'Contents': [
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=59)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_10.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },

                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-a5464564.mp4.stream.imu.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-sdfsdds5e.mp4',
                 'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 }
            ]
        }

        metadata_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-498375935_10.mp4_stream.json.zip',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4_stream.json.zip',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4_stream.json.zip'}

        video_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-afdgdffde_10.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-fgf12325e_12.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_InteriorRecorder-abc12325e_15.mp4'}
        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        metadata_ingestor.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

        start_time = datetime(year=2022, month=9, day=30, hour=20, minute=0)
        end_time = datetime(year=2022, month=9, day=30, hour=20, minute=30)

        metadata_chunks_set, video_chunks_set = metadata_ingestor._search_chunks_in_s3_path(
            reference_path, bucket, messageid, [".json.zip"], start_time=start_time, end_time=end_time)

        print(metadata_expected)
        print(metadata_chunks_set)
        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        metadata_ingestor.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    def test_search_chunks_in_s3_path2(self, metadata_ingestor):
        resp_mock = set()
        metadata_expected = set()
        video_expected = set()

        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        metadata_ingestor.check_if_s3_rcc_path_exists = Mock(return_value=(False, resp_mock))

        metadata_chunks_set, video_chunks_set = metadata_ingestor._search_chunks_in_s3_path(
            reference_path, bucket, messageid, [".json", ".zip"])

        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        metadata_ingestor.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    @patch("sdretriever.ingestor.metacontent.ContainerServices")
    def test_check_allparts_exist(self, mock_container_services, metadata_ingestor, msg_interior):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip'
        ]

        video_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_fgf12325e.mp4',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID_abc12325e.mp4'
        ]

        msg_interior.tenant = "rc_srx"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"
        msg_interior.uploadstarted = datetime(
            year=2022, month=8, day=1, hour=10)
        msg_interior.uploadfinished = datetime(
            year=2022, month=12, day=1, hour=10)

        mock_container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(
            return_value=True)

        lookup_mock = metadata_ingestor._get_chunks_lookup_paths = Mock(
            return_value=lookup_paths_reference)
        metadata_ingestor._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_expected[:2]), set(video_expected[:2])),
                (set([metadata_expected[2]]), set()),
                (set(), set([video_expected[2]]))
            ]
        )

        has_metadata, metadata = metadata_ingestor.check_allparts_exist(
            msg_interior)

        assert has_metadata
        assert metadata == set(metadata_expected)
        lookup_mock.assert_called_once_with(msg_interior)

    @patch("sdretriever.ingestor.metacontent.ContainerServices")
    def test_check_allparts_exist2(self, mock_container_services, metadata_ingestor, msg_interior):
        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/TEST_TYPE_TEST_ID_123.mp4.json.zip'
        ]

        video_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_fgf12325e.mp4',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID_abc12325e.mp4',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID_123.mp4'
        ]

        current_date_path = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/TEST_TYPE_TEST_ID']

        msg_interior.tenant = "rc_srx"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"
        msg_interior.uploadstarted = datetime(
            year=2022, month=8, day=1, hour=10)
        msg_interior.uploadfinished = datetime(
            year=2022, month=10, day=1, hour=10)

        mock_container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(
            return_value=True)

        lookup_mock = metadata_ingestor._get_chunks_lookup_paths = Mock(
            side_effect=[
                lookup_paths_reference,
                current_date_path
            ]
        )

        match_chunks_mock = metadata_ingestor._search_for_match_chunks = Mock(return_value=(
            True, {metadata_expected[3]}))

        metadata_ingestor._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_expected[:2]), set(video_expected[:2])),
                (set([metadata_expected[2]]), set()),
                (set(), set(video_expected[2:]))
            ]
        )

        has_metadata, metadata = metadata_ingestor.check_allparts_exist(
            msg_interior)

        assert has_metadata
        assert metadata == set(metadata_expected)
        # lookup_mock.assert_called_with(msg_interior)
        lookup_mock.assert_any_call(msg_interior)
        lookup_mock.assert_any_call(
            msg_interior, start_time=msg_interior.uploadfinished + timedelta(hours=1), end_time=ANY)
        match_chunks_mock.assert_called_with(current_date_path, ANY, ANY, ANY, ANY)

    @patch("sdretriever.ingestor.metacontent.ContainerServices")
    def test_check_allparts_exist3(self, mock_container_services, metadata_ingestor, msg_interior):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_given = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip'
        ]

        msg_interior.tenant = "rc_srx"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"
        msg_interior.uploadstarted = datetime(
            year=2022, month=8, day=1, hour=10)
        msg_interior.uploadfinished = datetime(
            year=2022, month=12, day=1, hour=10)

        mock_container_services.check_if_tenant_and_deviceid_exists_and_log_on_error = Mock(
            return_value=True)

        lookup_mock = metadata_ingestor._get_chunks_lookup_paths = Mock(
            return_value=lookup_paths_reference)
        metadata_ingestor._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_given[:2]), set()),
                (set([metadata_given[2]]), set()),
                (set(), set())
            ]
        )

        has_metadata, metadata = metadata_ingestor.check_allparts_exist(
            msg_interior)

        assert not has_metadata
        assert metadata == set()
        lookup_mock.assert_called_once_with(msg_interior)

    def test_search_for_match_chunks(self, metadata_ingestor):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID"
        ]
        meta_chunks = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=21/TEST_TYPE_TEST_ID_123.mp4.json'
        ]

        video_chunks = {
            'TEST_TYPE_TEST_ID_498375935.mp4',
            'TEST_TYPE_TEST_ID_fgf12325e.mp4',
            'TEST_TYPE_TEST_ID_abc12325e.mp4',
            'TEST_TYPE_TEST_ID_123.mp4'
        }

        bucket = "BUCKET"

        metadata_ingestor._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(meta_chunks[:2]), set()),
                (set(meta_chunks[2:]), set())
            ])
        all_metadata_found, metadata_chunks = metadata_ingestor._search_for_match_chunks(

            lookup_paths_reference, video_chunks, [".json", ".zip"], bucket, MagicMock())

        assert all_metadata_found
        assert metadata_chunks == set(meta_chunks)

        print(metadata_ingestor._search_chunks_in_s3_path.call_args_list)
        metadata_ingestor._search_chunks_in_s3_path.assert_any_call(
            ANY, bucket, match_chunk_extensions=[".json", ".zip"], messageid=ANY, recorder_type=ANY)

    def test_search_for_match_chunks_fail(self, metadata_ingestor):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID"
        ]
        meta_chunks = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=21/TEST_TYPE_TEST_ID_123.mp4.json'
        ]

        video_chunks = {
            'TEST_TYPE_TEST_ID_498375935.mp4',
            'TEST_TYPE_TEST_ID_fgf12325e.mp4',
            'TEST_TYPE_TEST_ID_abc12325e.mp4',
            'TEST_TYPE_TEST_ID_abc123768.mp4',
            'TEST_TYPE_TEST_ID_123.mp4'
        }

        bucket = "BUCKET"

        metadata_ingestor._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(meta_chunks[:2]), set()),
                (set(meta_chunks[2:]), set())
            ])

        all_metadata_found, metadata_chunks = metadata_ingestor._search_for_match_chunks(
            lookup_paths_reference, video_chunks, [".json", ".zip"], bucket, MagicMock())

        assert not all_metadata_found
        assert metadata_chunks == set(meta_chunks)

        metadata_ingestor._search_chunks_in_s3_path.assert_any_call(
            ANY, bucket, match_chunk_extensions=[".json", ".zip"], messageid=ANY, recorder_type=ANY)
