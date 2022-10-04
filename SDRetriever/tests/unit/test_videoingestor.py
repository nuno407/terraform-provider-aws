import pytest
from unittest import mock
from unittest.mock import MagicMock, ANY
from datetime import datetime
import os
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
        assert result == {"something":"something else"}
    
    @mock.patch("sdretriever.ingestor.subprocess.run")
    def test_ingest(self, mock_run, container_services, s3_client, sqs_client, sts_helper, msg_interior, video_bytes):
        mock_stdout = MagicMock()
        mock_stdout.configure_mock(
            **{"stdout.decode.return_value": '{"streams":[{"width":1920,"height":1080}],"format":{"duration":1000}}'.encode("utf-8")}
        )

        mock_run.return_value = mock_stdout
        obj = VideoIngestor(container_services, s3_client, sqs_client, sts_helper)

        db_record_data, request_metadata = obj.ingest(msg_interior)
        os.remove("input_video.mp4")
        
        expected_raw_path = "Debug_Lync/datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110.mp4"
        expected_db_record_data = {
            "_id": "datanauts_DATANAUTS_DEV_01_InteriorRecorder_1657297040802_1657297074110",
            "s3_path": container_services.raw_s3 + "/" + expected_raw_path,
            "recording_overview": {
                'length': "0:16:40",
                'resolution': "1920x1080",
                '#snapshots': "0",
                'snapshots_paths': {},
                },
            "MDF_available": "No",
            "sync_file_ext": "",
            "tenant":"datanauts",
            "deviceid":"DATANAUTS_DEV_01",
            "footagefrom":1657297040802,
            "footageto":1657297074110
        }
        video_from = datetime.fromtimestamp(msg_interior.footagefrom/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        video_to = datetime.fromtimestamp(msg_interior.footageto/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        container_services.get_kinesis_clip.assert_called_once_with(ANY,"datanauts_DATANAUTS_DEV_01_InteriorRecorder",video_from,video_to,"PRODUCER_TIMESTAMP")
        container_services.upload_file.assert_called_once_with(ANY,ANY,container_services.raw_s3,expected_raw_path)
        assert db_record_data == expected_db_record_data
        assert request_metadata == True
