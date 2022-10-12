import json
import os
import pickle
from datetime import datetime
from unittest.mock import ANY, Mock, call

import pytest
from sdretriever.ingestor import MetadataIngestor

@pytest.mark.unit
@pytest.mark.usefixtures("msg_interior", "container_services", "s3_client", "sqs_client", "sts_helper", "snapshot_rcc_folders", "snapshot_rcc_paths")
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
            "key1":"value1",
            "key2":"value2",
        }

    @pytest.fixture
    def obj(self, container_services, s3_client, sqs_client, sts_helper):
        return MetadataIngestor(container_services, s3_client, sqs_client, sts_helper)

    def test_json_raise_on_duplicates(self, obj):
        json_with_duplicates = '''{"chunk": {"pts_start": "25152337","pts_end": "26182733"},"chunk": {"utc_start": "1655453665526","utc_end": "1655453676953"}}'''
        result = json.loads(json_with_duplicates, object_pairs_hook=obj._json_raise_on_duplicates)
        expected_result = dict(chunk = {"pts_start": "25152337","pts_end": "26182733","utc_start": "1655453665526","utc_end": "1655453676953"})
        assert result == expected_result

    @Mock('gzip.decompress', side_effect=[b"{1:2,3:4}",b"{5:6,7:8}"])
    def test_get_metadata_chunks(self, msg_interior, obj, metadata_files):
        metadata_start_time = datetime.fromtimestamp(msg_interior.uploadstarted/1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(msg_interior.uploadfinished/1000.0).replace(microsecond=0, second=0, minute=0)
        files_to_download = ["datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/"+file for file in metadata_files]
        with open("artifacts/metadata_response.py", "rb") as f:
            response_dict = pickle.load(f)
        obj.check_if_exists = Mock(return_value=(True, response_dict))

        result = obj._get_metadata_chunks(metadata_start_time, metadata_end_time, msg_interior)

        obj.CS.download_file.assert_has_calls([call(ANY, obj.CS.rcc_info["s3_bucket"], file_name) for file_name in files_to_download], any_order=True)
        obj.check_if_exists.assert_called_with("datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/InteriorRecorder_InteriorRecorder-77d21ada-c79e-48c7-b582-cfc737773f26","dev-rcc-raw-video-data")
        assert result == {0: {1:2,3:4}, 1: {5:6,7:8}}

    def test_process_chunks_into_mdf(self, obj, metadata_chunks, msg_interior):
        resolution, pts, mdf_data = obj._process_chunks_into_mdf(metadata_chunks, msg_interior)
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json","r") as f:
            expected_metadata = json.load(f)
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/result.json","w") as f:
            json.dump(mdf_data, f)
        assert resolution == expected_metadata["resolution"]
        assert mdf_data == expected_metadata["frame"]
        assert pts == expected_metadata["chunk"]

    def test_upload_source_data(self, obj, source_data, msg_interior):
        s3_path = obj._upload_source_data(source_data,msg_interior, "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110")
        expected_client = ANY
        expected_source_bytes = bytes(json.dumps(source_data, ensure_ascii=False, indent=4).encode('UTF-8'))
        expected_path = "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110_metadata_full.json"
        expected_bucket = obj.CS.raw_s3
        obj.CS.upload_file.assert_called_with(expected_client, expected_source_bytes, expected_bucket, expected_path)
        assert s3_path == expected_path

    def test_ingest(self, obj, msg_interior, metadata_chunks, metadata_full):
        """ metadata_start_time = datetime.fromtimestamp(msg_interior.uploadstarted/1000.0).replace(microsecond=0, second=0, minute=0)
        metadata_end_time = datetime.fromtimestamp(msg_interior.uploadfinished/1000.0).replace(microsecond=0, second=0, minute=0)"""

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
        result = obj.ingest(msg_interior, "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110")

        obj._upload_source_data.assert_called_once_with(metadata_full,ANY, ANY)
        obj.CS.send_message.assert_called_once_with(ANY, "dev-terraform-queue-mdf-parser", ANY)
        assert result
