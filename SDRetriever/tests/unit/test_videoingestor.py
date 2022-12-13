import os
from datetime import datetime
from unittest import mock
from unittest.mock import ANY
from unittest.mock import MagicMock

import pytest

from sdretriever.ingestor import VideoIngestor


@pytest.mark.unit
@pytest.mark.usefixtures("container_services", "s3_client", "sqs_client", "sts_helper", "msg_interior")
class TestVideoIngestor():

    @mock.patch("sdretriever.ingestor.subprocess.run")
    def test_ffmpeg_probe_video(self, mock_run, video_bytes, container_services, s3_client, sqs_client, sts_helper):
        obj = VideoIngestor(container_services, s3_client, sqs_client, sts_helper)
        mock_stdout = MagicMock()
        mock_stdout.configure_mock(
            **{"stdout.decode.return_value": '{"something":"something else"}'.encode("utf-8")}
        )

        mock_run.return_value = mock_stdout
        result = obj._ffmpeg_probe_video(video_bytes)
        assert result == {"something": "something else"}

    @mock.patch("sdretriever.ingestor.subprocess.run")
    def test_ingest(self, mock_run, container_services, s3_client, sqs_client, sts_helper, msg_interior):
        mock_stdout = MagicMock()
        mock_stdout.configure_mock(
            **{"stdout.decode.return_value": '{"streams":[{"width":1920,"height":1080}],"format":{"duration":1000}}'.encode("utf-8")}
        )

        mock_run.return_value = mock_stdout
        obj = VideoIngestor(container_services, s3_client, sqs_client, sts_helper)

        db_record_data = obj.ingest(msg_interior)

        expected_raw_path = "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110.mp4"
        expected_db_record_data = {
            "_id": "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110",
            "s3_path": container_services.raw_s3 + "/" + expected_raw_path,
            "#snapshots": "0",
            "MDF_available": "No",
            "deviceid": "DATANAUTS_DEV_01",
            "footagefrom": 1657297040802,
            "footageto": 1657297074110,
            "length": "0:16:40",
            "media_type": "video",
            "resolution": "1920x1080",
            "snapshots_paths": [],
            "sync_file_ext": "",
            "tenant": "datanauts"
        }
        print(msg_interior.footagefrom, msg_interior.footageto)
        video_from = datetime.fromtimestamp(msg_interior.footagefrom / 1000.0)
        video_to = datetime.fromtimestamp(msg_interior.footageto / 1000.0)
        container_services.get_kinesis_clip.assert_called_once_with(
            ANY, "datanauts_DATANAUTS_DEV_01_InteriorRecorder", video_from, video_to, "PRODUCER_TIMESTAMP")
        container_services.upload_file.assert_called_once_with(ANY, ANY, container_services.raw_s3, expected_raw_path)
        assert db_record_data == expected_db_record_data

    @mock.patch("sdretriever.ingestor.subprocess.run")
    def test_ingest_request_training_recorder(
            self,
            mock_run,
            container_services,
            s3_client,
            sqs_client,
            sts_helper,
            msg_interior):
        mock_stdout = MagicMock()
        mock_stdout.configure_mock(
            **{"stdout.decode.return_value": '{"streams":[{"width":1920,"height":1080}],"format":{"duration":1000}}'.encode("utf-8")}
        )

        mock_run.return_value = mock_stdout
        obj = VideoIngestor(container_services, s3_client, sqs_client, sts_helper)

        obj.ingest(msg_interior, training_whitelist=['datanauts'], request_training_upload=True)
        expect_hq_request = {
            "streamName": f"{msg_interior.tenant}_{msg_interior.deviceid}_InteriorRecorder",
            "deviceId": msg_interior.deviceid,
            "footageFrom": msg_interior.footagefrom,
            "footageTo": msg_interior.footageto
        }
        expect_selector_input_queue = container_services.sqs_queues_list["HQ_Selector"]
        container_services.send_message.assert_called_once_with(ANY, expect_selector_input_queue, expect_hq_request)
