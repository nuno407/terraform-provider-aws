from ingestor import SnapshotIngestor
from datetime import datetime
from unittest.mock import Mock, ANY, call
import pytest

@pytest.mark.usefixtures("msg_snapshot", "container_services", "s3_client", "sqs_client", "sts_helper", "snapshot_rcc_folders", "snapshot_rcc_paths")
class TestSnapshotIngestor:
    
    @pytest.fixture
    def expected_rcc_folders(self):
        return [
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/', 
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=17/', 
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/'
        ]

    def test_snapshot_path_generator(self, msg_snapshot, container_services, s3_client, sqs_client, sts_helper, expected_rcc_folders):

        obj = SnapshotIngestor(container_services, s3_client, sqs_client, sts_helper)
        start = datetime.fromtimestamp(1660922264165/1000.0)
        end = datetime.fromtimestamp(1660929464176/1000.0)
        paths, searches = obj._snapshot_path_generator(msg_snapshot.tenant , msg_snapshot.deviceid, start, end)
        assert paths == expected_rcc_folders
        assert searches == {'datanauts': {'DATANAUTS_DEV_01': {'2022': {'08': {'19': {'18'}}}}}}

    def test_ingest_exists_on_devcloud(self, msg_snapshot, container_services, s3_client, sqs_client, sts_helper):

        obj = SnapshotIngestor(container_services, s3_client, sqs_client, sts_helper)
        obj.check_if_exists = Mock()
        obj.check_if_exists.side_effect = [(False,""),(True,""),(False,""),(True,"")]

        expected_rcc_paths = [
            'datanauts/DATANAUTS_DEV_01/year=2022/month=07/day=12/hour=17/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5.jpeg', 
            'datanauts/DATANAUTS_DEV_01/year=2022/month=07/day=12/hour=17/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6.jpeg', 
        ]
        expected_devcloud_paths = [
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
        ]
        
        container_services.download_file = Mock(return_value=b"snapshotbytes")
        obj.ingest(msg_snapshot)
        
        calls = [call(ANY,ANY,path) for path in expected_rcc_paths]
        container_services.download_file.assert_has_calls(calls, any_order = True)

        calls = [call(ANY,b"snapshotbytes",container_services.raw_s3,path) for path in expected_devcloud_paths]
        container_services.upload_file.assert_has_calls(calls, any_order = True)
        