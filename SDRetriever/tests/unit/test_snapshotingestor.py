from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, call, patch

import pytest

from sdretriever.ingestor import SnapshotIngestor


@pytest.mark.unit
@pytest.mark.usefixtures("msg_snapshot", "container_services", "s3_client", "sqs_client",
                         "sts_helper", "snapshot_rcc_folders", "snapshot_rcc_paths")
class TestSnapshotIngestor:

    @pytest.fixture
    def expected_rcc_folders(self):
        path1 = datetime.utcfromtimestamp(1660922264165 / 1000.0)
        year1 = path1.strftime("%Y")
        month1 = path1.strftime("%m")
        day1 = path1.strftime("%d")
        hour1 = path1.strftime("%H")
        path2 = path1 + timedelta(hours=1)
        year2 = path2.strftime("%Y")
        month2 = path2.strftime("%m")
        day2 = path2.strftime("%d")
        hour2 = path2.strftime("%H")
        path3 = path2 + timedelta(hours=1)
        year3 = path3.strftime("%Y")
        month3 = path3.strftime("%m")
        day3 = path3.strftime("%d")
        hour3 = path3.strftime("%H")
        return [
            f'datanauts/DATANAUTS_DEV_01/year={year1}/month={month1}/day={day1}/hour={hour1}/',
            f'datanauts/DATANAUTS_DEV_01/year={year2}/month={month2}/day={day2}/hour={hour2}/',
            f'datanauts/DATANAUTS_DEV_01/year={year3}/month={month3}/day={day3}/hour={hour3}/'
        ]

    def test_snapshot_path_generator(
            self,
            msg_snapshot,
            container_services,
            s3_client,
            sqs_client,
            sts_helper,
            expected_rcc_folders):

        obj = SnapshotIngestor(
            container_services, s3_client, sqs_client, sts_helper)
        start = datetime.utcfromtimestamp(1660922264165 / 1000.0)
        end = datetime.utcfromtimestamp(1660929464176 / 1000.0)
        paths = obj._snapshot_path_generator(
            msg_snapshot.tenant, msg_snapshot.deviceid, start, end)
        assert paths == expected_rcc_folders

    @patch("sdretriever.ingestor.ContainerServices")
    def test_ingest_exists_on_devcloud(
            self,
            ingestor_container_services,
            msg_snapshot,
            container_services,
            s3_client,
            sqs_client,
            sts_helper):

        obj = SnapshotIngestor(
            container_services, s3_client, sqs_client, sts_helper)
        ingestor_container_services.check_s3_file_exists.side_effect = [False, False]

        obj.check_if_s3_rcc_path_exists = Mock()
        obj.check_if_s3_rcc_path_exists.side_effect = [(True, ""), (True, "")]

        snapshot1_timestamp = datetime.utcfromtimestamp(1657642653000 / 1000.0)
        snapshot2_timestamp = datetime.utcfromtimestamp(1657642655000 / 1000.0)
        year1 = snapshot1_timestamp.strftime("%Y")
        month1 = snapshot1_timestamp.strftime("%m")
        day1 = snapshot1_timestamp.strftime("%d")
        hour1 = snapshot1_timestamp.strftime("%H")
        year2 = snapshot2_timestamp.strftime("%Y")
        month2 = snapshot2_timestamp.strftime("%m")
        day2 = snapshot2_timestamp.strftime("%d")
        hour2 = snapshot2_timestamp.strftime("%H")

        expected_rcc_paths = [
            f'datanauts/DATANAUTS_DEV_01/year={year1}/month={month1}/day={day1}/hour={hour1}/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5.jpeg',
            f'datanauts/DATANAUTS_DEV_01/year={year2}/month={month2}/day={day2}/hour={hour2}/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6.jpeg',
        ]
        expected_devcloud_paths = [
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
            "Debug_Lync/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
        ]

        container_services.download_file = Mock(return_value=b"snapshotbytes")
        obj.ingest(msg_snapshot)

        calls = [call(ANY, ANY, path) for path in expected_rcc_paths]
        container_services.download_file.assert_has_calls(
            calls, any_order=True)

        calls = [call(ANY, b"snapshotbytes", container_services.raw_s3, path)
                 for path in expected_devcloud_paths]
        container_services.upload_file.assert_has_calls(calls, any_order=True)
