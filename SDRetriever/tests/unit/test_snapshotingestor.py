# type: ignore
from datetime import datetime, timedelta
from unittest.mock import ANY, Mock, call, patch

import pytest

import hashlib
from sdretriever.ingestor.snapshot import SnapshotIngestor
from sdretriever.message.message import Chunk
from sdretriever.message.snapshot import SnapshotMessage


@pytest.mark.unit
@pytest.mark.usefixtures("msg_snapshot", "container_services", "s3_client", "sqs_client",
                         "sts_helper", "snapshot_rcc_folders", "snapshot_rcc_paths")
def msg_helper() -> SnapshotMessage:
    msg = Mock()
    msg.tenant = "datanauts"
    msg.deviceid = "DATANAUTS_DEV_01"
    msg.messageid = "bd6261e1-d069-4443-9727-c8a7abfa80ee"
    msg.senttimestamp = "1657725925161"
    chunks = [{"uuid": "TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5.jpeg",
               "upload_status": "UPLOAD_STATUS__SELECTED_FOR_UPLOAD",
               "start_timestamp_ms": 1657642653000,
               "end_timestamp_ms": 1657642654000,
               "payload_size": 422576},
              {"uuid": "TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6.jpeg",
               "upload_status": "UPLOAD_STATUS__SELECTED_FOR_UPLOAD",
               "start_timestamp_ms": 1657642655000,
               "end_timestamp_ms": 1657642656000,
               "payload_size": 455856}]
    msg.chunks = [Chunk(a) for a in chunks]
    return msg


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

    @patch("sdretriever.ingestor.snapshot.ContainerServices")
    @pytest.mark.unit
    @pytest.mark.parametrize("msg_snapshot,is_present_devCloud,is_available_rcc,message_db_sent,path_files_upload,return_value", [
        # Success
        (
            msg_helper(),
            [False, False],
            [True, True, True, True],
            [{"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000",
                "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
                "deviceid": "DATANAUTS_DEV_01",
                "timestamp": 1657642653000,
                "tenant": "datanauts",
                "media_type": "image",
                "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000".encode("utf-8")).hexdigest(),
              },
             {"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000",
              "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
              "deviceid": "DATANAUTS_DEV_01",
              "timestamp": 1657642655000,
              "tenant": "datanauts",
              "media_type": "image",
              "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000".encode("utf-8")).hexdigest()
              }
             ],
            [
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000_metadata.json",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000_metadata.json"
            ],
            False
        ), (
            msg_helper(),
            [False, True],
            [True, True],
            [{"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000",
                "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
                "deviceid": "DATANAUTS_DEV_01",
                "timestamp": 1657642653000,
                "tenant": "datanauts",
                "media_type": "image",
                "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000".encode("utf-8")).hexdigest(),
              }
             ],
            [
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5_1657642653000_metadata.json"
            ],
            False
        ),
        (
            msg_helper(),
            [False, False],
            [False, True, True],
            [{"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000",
              "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
              "deviceid": "DATANAUTS_DEV_01",
              "timestamp": 1657642655000,
              "tenant": "datanauts",
              "media_type": "image",
              "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000".encode("utf-8")).hexdigest()
              }
             ],
            [
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000_metadata.json"
            ],
            True
        ),
        (
            msg_helper(),
            [False, False],
            [True, False, True, True],
            [{"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000",
              "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
              "deviceid": "DATANAUTS_DEV_01",
              "timestamp": 1657642655000,
              "tenant": "datanauts",
              "media_type": "image",
              "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000".encode("utf-8")).hexdigest()
              }
             ],
            [
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000_metadata.json"
            ],
            True
        ),
        (
            msg_helper(),
            [True, False],
            [True, True],
            [{"_id": "datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000",
              "s3_path": "bucket/datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
              "deviceid": "DATANAUTS_DEV_01",
              "timestamp": 1657642655000,
              "tenant": "datanauts",
              "media_type": "image",
              "internal_message_reference_id": hashlib.sha256("datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000".encode("utf-8")).hexdigest()
              }
             ],
            [
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000.jpeg",
                "datanauts/datanauts_DATANAUTS_DEV_01_TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6_1657642655000_metadata.json"
            ],
            False
        )
    ])
    def test_ingest_exists_on_devcloud(
            self,
            container_services,
            sqs_client,
            sts_helper,
            s3_client,
            msg_snapshot: dict,
            is_present_devCloud: list[bool],
            is_available_rcc: list[bool],
            message_db_sent: list[dict],
            path_files_upload: list[str],
            return_value: bool):

        container_services.raw_s3 = "bucket"

        obj = SnapshotIngestor(
            container_services, s3_client, sqs_client, sts_helper)
        container_services.check_s3_file_exists.side_effect = is_present_devCloud

        mock_files = [b'mock_data' for _ in filter(lambda x: x, is_available_rcc)]
        obj.get_file_in_rcc = Mock()
        obj.get_file_in_rcc.side_effect = [b'mock_data' if avbl else FileNotFoundError for avbl in is_available_rcc]

        rtn = obj.ingest(msg_snapshot)

        # Check file uploaded
        files_upload = [call(ANY, file, "bucket", path) for path, file in zip(path_files_upload, mock_files)]
        container_services.upload_file.assert_has_calls(
            files_upload)

        # Check sqs update
        queue_update = [call(ANY, ANY, message) for message in message_db_sent]
        obj.container_svcs.send_message.assert_has_calls(queue_update)

        # Check return
        assert rtn == return_value
