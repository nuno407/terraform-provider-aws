import json
import os
import pickle
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, call, patch

import pytest

from sdretriever.ingestor import MetadataIngestor


@pytest.mark.unit
@pytest.mark.usefixtures("msg_interior", "container_services", "s3_client", "sqs_client",
                         "sts_helper", "snapshot_rcc_folders", "snapshot_rcc_paths", "list_s3_objects")
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
    def obj(self, container_services, s3_client, sqs_client, sts_helper):
        return MetadataIngestor(container_services, s3_client, sqs_client, sts_helper)

    def test_json_raise_on_duplicates(self, obj):
        json_with_duplicates = '''{"chunk": {"pts_start": "25152337","pts_end": "26182733"},"chunk": {"utc_start": "1655453665526","utc_end": "1655453676953"}}'''
        result = json.loads(json_with_duplicates,
                            object_pairs_hook=obj._json_raise_on_duplicates)
        expected_result = dict(chunk={"pts_start": "25152337", "pts_end": "26182733",
                               "utc_start": "1655453665526", "utc_end": "1655453676953"})
        assert result == expected_result

    @Mock('gzip.decompress', side_effect=[b"{1:2,3:4}", b"{5:6,7:8}"])
    def test_get_metadata_chunks(self, msg_interior, obj, metadata_files):
        metadata_start_time = datetime.fromtimestamp(
            msg_interior.uploadstarted / 1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(
            msg_interior.uploadfinished / 1000.0).replace(microsecond=0, second=0, minute=0)
        files_to_download = [
            "datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/" + file for file in metadata_files]
        with open("artifacts/metadata_response.pickle", "rb") as f:
            response_dict = pickle.load(f)
        obj.check_if_exists = Mock(return_value=(True, response_dict))

        result = obj._get_metadata_chunks(
            metadata_start_time, metadata_end_time, msg_interior)

        obj.CS.download_file.assert_has_calls(
            [call(ANY, obj.CS.rcc_info["s3_bucket"], file_name) for file_name in files_to_download], any_order=True)
        obj.check_if_exists.assert_called_with(
            "datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/InteriorRecorder_InteriorRecorder-77d21ada-c79e-48c7-b582-cfc737773f26",
            "dev-rcc-raw-video-data")
        assert result == {0: {1: 2, 3: 4}, 1: {5: 6, 7: 8}}

    def test_process_chunks_into_mdf(self, obj, metadata_chunks, msg_interior):
        resolution, pts, mdf_data = obj._process_chunks_into_mdf(
            metadata_chunks, msg_interior)
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json", "r") as f:
            expected_metadata = json.load(f)
        assert resolution == expected_metadata["resolution"]
        assert mdf_data == expected_metadata["frame"]
        assert pts == expected_metadata["chunk"]

    def test_upload_source_data(self, obj, source_data, msg_interior):
        s3_path = obj._upload_source_data(
            source_data, msg_interior, "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110")
        expected_client = ANY
        expected_source_bytes = bytes(json.dumps(
            source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        expected_path = "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json"
        expected_bucket = obj.CS.raw_s3
        obj.CS.upload_file.assert_called_with(
            expected_client, expected_source_bytes, expected_bucket, expected_path)
        assert s3_path == expected_path

    def test_ingest(self, obj, msg_interior, metadata_chunks, metadata_full):
        """ metadata_start_time = datetime.fromtimestamp(msg_interior.uploadstarted/1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(msg_interior.uploadfinished/1000.0).replace(microsecond=0, second=0, minute=0)"""

        mock_chunks_path = Mock()

        obj._get_metadata_chunks = Mock(return_value=metadata_chunks)
        obj._process_chunks_into_mdf = Mock(return_value=(
            metadata_full["resolution"],
            metadata_full["chunk"],
            metadata_full["frame"]
        ))
        obj._upload_source_data = Mock(return_value=(
            "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110",
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json"
        ))
        obj.CS.send_message = Mock()
        os.environ["QUEUE_MDFP"] = "dev-terraform-queue-mdf-parser"
        result = obj.ingest(
            msg_interior, "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110", mock_chunks_path)

        obj._upload_source_data.assert_called_once_with(
            metadata_full, ANY, ANY)
        obj.CS.send_message.assert_called_once_with(
            ANY, "dev-terraform-queue-mdf-parser", ANY)

        obj._get_metadata_chunks.assert_called_once_with(ANY, mock_chunks_path)
        assert result

    def test_get_chunks_lookup_paths(self, obj, msg_interior):
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
        obj._discover_s3_subfolders = Mock(
            return_value=paths_reference)

        start_time = datetime(year=2022, month=8, day=30, hour=20, minute=10)
        end_time = datetime(year=2022, month=11, day=30, hour=20)

        msg_interior.uploadstarted = start_time
        msg_interior.uploadfinished = end_time
        msg_interior.tenant = "TEST_TENANT"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"

        paths_to_test = set(obj._get_chunks_lookup_paths(msg_interior))

        assert paths_to_test == expected_result
        obj._discover_s3_subfolders.assert_called_once_with(
            "TEST_TENANT/TEST_DEVICE_ID/", ANY, ANY, start_time.replace(minute=0), end_time)

    def test_get_chunks_lookup_paths2(self, obj, msg_interior):
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
        obj._discover_s3_subfolders = Mock(
            return_value=paths_reference)

        start_time = datetime(year=2022, month=8, day=30, hour=20, minute=10)
        end_time = datetime(year=2022, month=11, day=30, hour=20)

        msg_interior.tenant = "TEST_TENANT"
        msg_interior.deviceid = "TEST_DEVICE_ID"
        msg_interior.recordingid = "TEST_ID"
        msg_interior.recording_type = "TEST_TYPE"

        paths_to_test = set(obj._get_chunks_lookup_paths(
            msg_interior, start_time, end_time))

        assert paths_to_test == expected_result
        obj._discover_s3_subfolders.assert_called_once_with(
            "TEST_TENANT/TEST_DEVICE_ID/", ANY, ANY, start_time.replace(minute=0), end_time)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_discover_s3_subfolders(self, mock_container_services, obj, s3_client, list_s3_objects):

        list_s3_objects_func, path_list = list_s3_objects
        paths_reference = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=09/day=30/hour=20/TEST_ID_TEST_TYPE",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_ID_TEST_TYPE"
        ]
        path_list.extend(paths_reference)
        mock_container_services.list_s3_objects.side_effect = list_s3_objects_func

        wanted_result = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/"
        ]

        folder_reference = "TEST_TENANT/TEST_DEVICE_ID/"
        bucket_reference = "UNUSED"
        start_datetime_reference = datetime(year=2022, month=8, day=20, hour=1)
        end_datetime_reference = datetime(year=2022, month=10, day=1, hour=1)

        paths = obj._discover_s3_subfolders(folder_reference, bucket_reference, s3_client,
                                            start_time=start_datetime_reference, end_time=end_datetime_reference)

        assert set(paths) == set(wanted_result)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_discover_s3_subfolders2(self, mock_container_services, obj, s3_client, list_s3_objects):

        list_s3_objects_func, path_list = list_s3_objects
        paths_reference = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=09/day=30/hour=20/TEST_ID_TEST_TYPE",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_ID_TEST_TYPE"
        ]
        path_list.extend(paths_reference)
        mock_container_services.list_s3_objects.side_effect = list_s3_objects_func

        wanted_result = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/"
        ]

        folder_reference = "TEST_TENANT/TEST_DEVICE_ID/"
        bucket_reference = "UNUSED"
        start_datetime_reference = datetime(
            year=2022, month=9, day=30, hour=22, minute=10)
        end_datetime_reference = datetime(year=2022, month=10, day=1, hour=1)

        paths = obj._discover_s3_subfolders(folder_reference, bucket_reference, s3_client,
                                            start_time=start_datetime_reference, end_time=end_datetime_reference)

        paths = {path for path in paths}

        assert paths == set(wanted_result)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_discover_s3_subfolders3(self, mock_container_services, obj, s3_client, list_s3_objects):

        list_s3_objects_func, path_list = list_s3_objects
        paths_reference = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=05/day=25/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=10/day=30/hour=20/TEST_ID_TEST_TYPE1",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=21/TEST_ID_TEST_TYPE2",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=22/TEST_ID_TEST_TYPE3",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/TEST_ID_TEST_TYPE4",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=00/TEST_ID_TEST_TYPE5",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=01/TEST_ID_TEST_TYPE6",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=02/TEST_ID_TEST_TYPE7",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=03/TEST_ID_TEST_TYPE8",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=03/day=01/hour=03/TEST_ID_TEST_TYPE8"
        ]
        path_list.extend(paths_reference)
        mock_container_services.list_s3_objects.side_effect = list_s3_objects_func

        wanted_result = [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=01/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=02/",
        ]

        folder_reference = "TEST_TENANT/TEST_DEVICE_ID/"
        bucket_reference = "UNUSED"
        start_datetime_reference = datetime(
            year=2022, month=10, day=30, hour=22, minute=10)
        end_datetime_reference = datetime(year=2023, month=2, day=28, hour=2)

        paths = obj._discover_s3_subfolders(folder_reference, bucket_reference, s3_client,
                                            start_time=start_datetime_reference, end_time=end_datetime_reference)

        paths = {path for path in paths}

        assert paths == set(wanted_result)

    def test_search_chunks_in_s3_path(self, obj):
        resp_mock = {
            'Contents': [
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4_stream.json',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_afdgdffde.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4.zip',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4',
                        'LastModified': datetime(year=2022, month=10, day=10)
                 }
            ]
        }

        metadata_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4_stream.json',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4_stream.json.zip',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4.zip'}

        video_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_afdgdffde.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4'
        }
        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        obj.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

        metadata_chunks_set, video_chunks_set = obj._search_chunks_in_s3_path(
            reference_path, bucket, messageid)

        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        obj.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    def test_search_chunks_in_s3_path_time_bound(self, obj):
        resp_mock = {
            'Contents': [
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=59)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4_stream.json',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_afdgdffde.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4_stream.json.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4.zip',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 },
                {'Key': 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4',
                        'LastModified': datetime(year=2022, month=9, day=30, hour=20, minute=1)
                 }
            ]
        }

        metadata_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_498375935.mp4_stream.json',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4_stream.json.zip',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4.zip'}

        video_expected = {
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_afdgdffde.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_fgf12325e.mp4',
            'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/InteriorRecorder_abc12325e.mp4'
        }
        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        obj.check_if_s3_rcc_path_exists = Mock(return_value=(True, resp_mock))

        start_time = datetime(year=2022, month=9, day=30, hour=20, minute=0)
        end_time = datetime(year=2022, month=9, day=30, hour=20, minute=30)

        metadata_chunks_set, video_chunks_set = obj._search_chunks_in_s3_path(
            reference_path, bucket, messageid, start_time=start_time, end_time=end_time)

        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        obj.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    def test_search_chunks_in_s3_path2(self, obj):
        resp_mock = set()
        metadata_expected = set()
        video_expected = set()

        reference_path = 'TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/'
        bucket = "BUCKET"
        messageid = "msg"
        obj.check_if_s3_rcc_path_exists = Mock(return_value=(False, resp_mock))

        metadata_chunks_set, video_chunks_set = obj._search_chunks_in_s3_path(
            reference_path, bucket, messageid)

        assert metadata_expected == metadata_chunks_set
        assert video_expected == video_chunks_set
        obj.check_if_s3_rcc_path_exists.assert_called_once_with(
            reference_path, bucket, messageid=messageid, max_s3_api_calls=5)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_check_metadata_exists_and_is_complete(self, mock_container_services, obj, msg_interior):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.zip',
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

        lookup_mock = obj._get_chunks_lookup_paths = Mock(
            return_value=lookup_paths_reference)
        obj._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_expected[:2]), set(video_expected[:2])),
                (set([metadata_expected[2]]), set()),
                (set(), set([video_expected[2]]))
            ]
        )

        has_metadata, metadata = obj.check_metadata_exists_and_is_complete(
            msg_interior)

        assert has_metadata
        assert metadata == set(metadata_expected)
        lookup_mock.assert_called_once_with(msg_interior)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_check_metadata_exists_and_is_complete2(self, mock_container_services, obj, msg_interior):
        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/TEST_TYPE_TEST_ID_123.mp4.json'
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

        lookup_mock = obj._get_chunks_lookup_paths = Mock(
            side_effect=[
                lookup_paths_reference,
                current_date_path
            ]
        )

        match_chunks_mock = obj._search_for_match_chunks = Mock(return_value=(
            True, {metadata_expected[3]}))

        obj._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_expected[:2]), set(video_expected[:2])),
                (set([metadata_expected[2]]), set()),
                (set(), set(video_expected[2:]))
            ]
        )

        has_metadata, metadata = obj.check_metadata_exists_and_is_complete(
            msg_interior)

        assert has_metadata
        assert metadata == set(metadata_expected)
        # lookup_mock.assert_called_with(msg_interior)
        lookup_mock.assert_any_call(msg_interior)
        lookup_mock.assert_any_call(
            msg_interior, start_time=msg_interior.uploadfinished + timedelta(hours=1), end_time=ANY)
        match_chunks_mock.assert_called_with(current_date_path, ANY, ANY, ANY)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_check_metadata_exists_and_is_complete3(self, mock_container_services, obj, msg_interior):

        lookup_paths_reference = [
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID",
            "rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_TYPE_TEST_ID"
        ]

        metadata_expected = [
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_498375935.mp4_stream.json',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/TEST_TYPE_TEST_ID_abc12325e.mp4.zip',
            'rc_srx/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_TYPE_TEST_ID_fgf12325e.mp4_stream.json.zip'
        ]

        video_expected = [
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

        lookup_mock = obj._get_chunks_lookup_paths = Mock(
            return_value=lookup_paths_reference)
        obj._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(metadata_expected[:2]), set()),
                (set([metadata_expected[2]]), set()),
                (set(), set())
            ]
        )

        has_metadata, metadata = obj.check_metadata_exists_and_is_complete(
            msg_interior)

        assert not has_metadata
        assert metadata == set()
        lookup_mock.assert_called_once_with(msg_interior)

    def test_search_for_match_chunks(self, obj):

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
        messageID = "messageID"

        obj._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(meta_chunks[:2]), set()),
                (set(meta_chunks[2:]), set())
            ])

        all_metadata_found, metadata_chunks = obj._search_for_match_chunks(
            lookup_paths_reference, video_chunks, bucket, messageID)

        assert all_metadata_found
        assert metadata_chunks == set(meta_chunks)
        obj._search_chunks_in_s3_path.assert_any_call(
            ANY, bucket, messageid=messageID)

    def test_search_for_match_chunks_fail(self, obj):

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
        messageID = "messageID"

        obj._search_chunks_in_s3_path = Mock(
            side_effect=[
                (set(meta_chunks[:2]), set()),
                (set(meta_chunks[2:]), set())
            ])

        all_metadata_found, metadata_chunks = obj._search_for_match_chunks(
            lookup_paths_reference, video_chunks, bucket, messageID)

        assert not all_metadata_found
        assert metadata_chunks == set(meta_chunks)
        obj._search_chunks_in_s3_path.assert_any_call(
            ANY, bucket, messageid=messageID)
