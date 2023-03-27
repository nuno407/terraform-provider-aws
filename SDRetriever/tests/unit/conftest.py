# type: ignore
import gzip
import json
import os
from datetime import datetime
from typing import Any
from typing import Callable
from typing import cast
from typing import List, Union
from typing import Optional
from unittest.mock import Mock

import pytest
from mypy_boto3_s3 import S3Client

from sdretriever.ingestor.metadata import MetadataIngestor
from sdretriever.ingestor.snapshot import SnapshotIngestor
from sdretriever.ingestor.imu import IMUIngestor
from sdretriever.ingestor.video import VideoIngestor
from sdretriever.ingestor.metacontent import MetacontentChunk
from sdretriever.message.message import Chunk
from sdretriever.message.snapshot import SnapshotMessage
from sdretriever.message.video import VideoMessage
from sdretriever.config import SDRetrieverConfig


@pytest.fixture
def container_services() -> Mock:
    cs = Mock()
    cs.upload_file = Mock()
    cs.raw_s3 = "dev-rcd-raw-video-files"
    cs.sdr_folder = {"debug": "Debug_Lync/",
                     "fut2": "FUT2/", "driver_pr": "Driver-PR/"}
    cs.send_message = Mock()
    cs.get_kinesis_clip = Mock(return_value=(b"These are some video bytes", datetime.fromtimestamp(
        1657297040802 / 1000.0), datetime.fromtimestamp(1657297074110 / 1000.0)))
    cs.sqs_queues_list = {
        "SDM": "dev-terraform-queue-s3-sdm",
        "Anonymize": "dev-terraform-queue-anonymize",
        "API_Anonymize": "dev-terraform-queue-api-anonymize",
        "ACAPI": "",
        "CHC": "dev-terraform-queue-chc",
        "API_CHC": "dev-terraform-queue-api-chc",
        "SDRetriever": "dev-terraform-queue-download",
        "Selector": "dev-terraform-queue-selector",
        "HQ_Selector": "dev-terraform-queue-hq-request",
        "Metadata": "dev-terraform-queue-metadata",
        "Output": "dev-terraform-queue-output",
        "MDFParser": "dev-terraform-queue-mdf-parser"
    }
    cs.RCC_S3_CLIENT = s3_client
    cs.rcc_info = {"s3_bucket": "rcc-dev-device-data"}
    return cs


@pytest.fixture
def s3_client():
    return Mock()


@pytest.fixture
def sqs_client():
    return Mock()


@pytest.fixture
def sts_helper():
    sts_helper = Mock()
    rcc_creds = {
        "AccessKeyId": "",
        "SecretAccessKey": "",
        "SessionToken": "",
    }
    sts_helper.get_credentials = Mock(return_value=rcc_creds)

    return sts_helper

# Video fixtures


@pytest.fixture
def video_bytes():
    return "these are some video bytes".encode("utf-8")


@pytest.fixture
def msg_interior(metadata_full) -> VideoMessage:
    msg = Mock()
    msg.raw_message = metadata_full["message"]
    msg.message = '\\"streamName\\":\\"datanauts_DATANAUTS_DEV_01_InteriorRecorder\\",\\"footageFrom\\":1657297040802,\\"footageTo\\":1657297074110,\\"uploadStarted\\":1657297078505,\\"uploadFinished\\":1657297083111}',
    msg.streamname = "datanauts_DATANAUTS_DEV_01_InteriorRecorder"
    msg.footagefrom = 1657297040802
    msg.footageto = 1657297074110
    msg.uploadstarted = datetime.fromtimestamp(
        1657297078505 / 1000.0)  # 1657297078505
    msg.uploadfinished = datetime.fromtimestamp(
        1657297083111 / 1000.0)  # 1657297083111
    msg.messageid = "6a079463-ef6a-4511-99a0-d35b64bc1e80"
    msg.tenant = "datanauts"
    msg.deviceid = "DATANAUTS_DEV_01"
    msg.senttimestamp = "1657297091751"
    msg.topicarn = "dev-video-footage-events"
    msg.video_recording_type = Mock(return_value='InteriorRecorder')
    msg.recording_type = 'InteriorRecorder'
    msg.recordingid = "InteriorRecorder_InteriorRecorder-77d21ada-c79e-48c7-b582-cfc737773f26"
    return msg


@pytest.fixture
def metadata_files() -> List[str]:
    return [
        "InteriorRecorder_InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63_10.mp4._stream2_20230301132144_9_metadata.json.zip",
        "InteriorRecorder_InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63_11.mp4._stream2_20230301132155_10_metadata.json.zip",
        "InteriorRecorder_InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63_12.mp4._stream2_20230301132206_11_metadata.json.zip",
        "InteriorRecorder_InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63_13.mp4._stream2_20230301132218_12_metadata.json.zip",
        "InteriorRecorder_InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63_14.mp4._stream2_20230301132229_13_metadata.json.zip"]


@pytest.fixture
def list_s3_objects() -> Callable[[str, str, S3Client, Optional[str]], Any]:
    paths_reference = []

    def mock_list_s3_objects(s3_path: str, bucket: str, s3_client: S3Client, delimiter: Optional[str] = None):
        result_set = set()
        for reference in paths_reference:
            if reference.startswith(s3_path):
                # Strip the path to contain until the next the delimiter
                pos_delimiter = reference[len(s3_path):].find(delimiter)
                if pos_delimiter != -1:
                    final_pos = 1 + pos_delimiter + len(s3_path)
                else:
                    final_pos = len(reference)

                result_set.add(reference[:final_pos])

        result = {}
        result['CommonPrefixes'] = []
        for each in result_set:
            result['CommonPrefixes'].append({'Prefix': each})
        return result

    return mock_list_s3_objects, paths_reference


@pytest.fixture
def metadata_full() -> dict:
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/ridecare_companion_fut_rc_srx_prod_a8c08d7b1c19f930892ba6b56fc885b7ffbd3275_InteriorRecorder_1677676914505_1677676967654_metadata_full.json", "r") as f:
        mdf = json.load(f, object_pairs_hook=MetadataIngestor._json_raise_on_duplicates)
    return mdf


def helper_read_chunks(metadata_files, file_type="metadata", decode=True) -> Union[list[str], list[bytearray]]:
    chunks: list[str] = list()
    for file in metadata_files:
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/{file_type}_raw_chunks/{file}", "rb") as f:
            compressed_metadata_file = f.read()
            metadata = gzip.decompress(compressed_metadata_file)
            if decode:
                chunks.append(metadata.decode(
                    "utf-8"))
            else:
                chunks.append(metadata)
    return chunks


@pytest.fixture
def metadata_chunks(metadata_files) -> dict[int, dict]:
    chunks = dict()
    for i, data_str in enumerate(helper_read_chunks(metadata_files)):
        chunks[i] = json.loads(data_str, object_pairs_hook=MetadataIngestor._json_raise_on_duplicates)
    return chunks


@pytest.fixture
def metacontent_chunks_metadata(metadata_files) -> list[MetacontentChunk]:
    chunks = helper_read_chunks(metadata_files=metadata_files)
    return [MetacontentChunk(data=chunk, filename=f"MOCKED_FILE_NAME_{i}") for i, chunk in enumerate(chunks)]


@pytest.fixture
def message_metadata(metadata_full) -> VideoMessage:
    msg = Mock()
    msg.raw_message = metadata_full["message"]
    msg.message = '\\"streamName\\\":\\\"ridecare_companion_fut_rc_srx_prod_a8c08d7b1c19f930892ba6b56fc885b7ffbd3275_InteriorRecorder\\\",\\\"footageFrom\\\":1677676905921,\\\"footageTo\\\":1677676966721,\\\"uploadStarted\\\":1677677746823,\\\"uploadFinished\\\":1677677758776}',
    msg.streamname = "ridecare_companion_fut_rc_srx_prod_a8c08d7b1c19f930892ba6b56fc885b7ffbd3275_InteriorRecorder"
    msg.footagefrom = 1677676905921
    msg.footageto = 1677676966721
    msg.uploadstarted = datetime.fromtimestamp(
        1677677746823 / 1000.0)  # 1657297078505
    msg.uploadfinished = datetime.fromtimestamp(
        1677677758776 / 1000.0)  # 1657297083111
    msg.messageid = "1804e0d1-fe9f-4138-937d-20c6e1e0ece7"
    msg.tenant = "ridecare_companion_fut"
    msg.deviceid = "rc_srx_prod_a8c08d7b1c19f930892ba6b56fc885b7ffbd3275"
    msg.senttimestamp = "1677677777045"
    msg.topicarn = "arn:aws:sns:eu-central-1:736745337734:prod-video-footage-events"
    msg.video_recording_type = Mock(return_value='InteriorRecorder')
    msg.recording_type = 'InteriorRecorder'
    msg.recordingid = "InteriorRecorder-fb6cd479-adcf-4af3-a216-1c96ed340d63"
    return cast(VideoMessage, msg)


# Snapshot fixtures
@pytest.fixture
def snapshot_rcc_folders() -> List[str]:
    # folders where to look for - possible snapshot locations
    return [
        'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/',
        'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=17/',
        'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=18/'
    ]


@pytest.fixture
def snapshot_rcc_paths() -> List[str]:
    # paths where the snapshots are located
    return [
        'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_5.jpeg',
        'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/TrainingMultiSnapshot_TrainingMultiSnapshot-552001f5-0d63-461e-9c4c-0ef3d74995e5_6.jpeg',
    ]


@pytest.fixture
def msg_snapshot() -> SnapshotMessage:
    msg = Mock()
    msg.tenant = "datanauts"
    msg.deviceid = "DATANAUTS_DEV_01"
    msg.messageid = "bd6261e1-d069-4443-9727-c8a7abfa80ee"
    msg.deviceid = "DATANAUTS_DEV_01"
    msg.senttimestamp = "1657725925161"
    msg.rcc_info = {
        "role": "arn:aws:iam::736745337734:role/prod-DevCloud",
        "s3_bucket": "rcc-prod-device-data"
    }
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


@pytest.fixture
def config_yml():
    _config = SDRetrieverConfig.load_config_from_yaml_file(
        os.environ.get('CONFIG_FILE', './config/config.yml'))
    return _config


@pytest.fixture
def imu_files() -> List[str]:
    return [
        "TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_56.mp4._stream1_20230221171736_153_imu_raw.csv.zip",
        "TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_57.mp4._stream1_20230221171748_154_imu_raw.csv.zip",
        "TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_58.mp4._stream1_20230221171759_155_imu_raw.csv.zip"]


@pytest.fixture
def imu_chunks(imu_files: list[str]) -> list[MetacontentChunk]:
    chunks = helper_read_chunks(imu_files, "imu", False)
    return [MetacontentChunk(imu_file, imu_path) for imu_file, imu_path in zip(chunks, imu_files)]


@pytest.fixture
def imu_ingestor(container_services, s3_client, sqs_client, sts_helper) -> IMUIngestor:
    return IMUIngestor(container_services, s3_client, sqs_client, sts_helper)


@pytest.fixture
def imu_full(imu_file="TrainingRecorder_TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749_imu_concat.csv") -> bytearray:
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/{imu_file}", "rb") as f:
        imu_full = f.read()
    return imu_full


@pytest.fixture
def training_message_metadata(metadata_full) -> VideoMessage:
    msg = Mock()
    msg.raw_message = metadata_full["message"]
    msg.message = '\\"streamName\\\":\\\"datanauts_DATANAUTS_TEST_02_TrainingRecorder\\\",\\\"footageFrom\\\":1677676905921,\\\"footageTo\\\":1677676966721,\\\"uploadStarted\\\":1677677746823,\\\"uploadFinished\\\":1677677758776}',
    msg.streamname = "datanauts_DATANAUTS_TEST_02_TrainingRecorder"
    msg.footagefrom = 1677676905921
    msg.footageto = 1677676966721
    msg.uploadstarted = datetime.fromtimestamp(
        1677677746823 / 1000.0)  # 1657297078505
    msg.uploadfinished = datetime.fromtimestamp(
        1677677758776 / 1000.0)  # 1657297083111
    msg.messageid = "1804e0d1-fe9f-4138-937d-20c6e1e0ece7"
    msg.tenant = "datanauts"
    msg.deviceid = "DATANAUTS_TEST_02"
    msg.senttimestamp = "1677677777045"
    msg.topicarn = "arn:aws:sns:eu-central-1:736745337734:prod-video-footage-events"
    msg.video_recording_type = Mock(return_value='TrainingRecorder')
    msg.recording_type = 'TrainingRecorder'
    msg.recordingid = "TrainingRecorder-7d5d57c3-aa1d-4554-8d44-7a3e13227749"
    return cast(VideoMessage, msg)


@pytest.fixture
def metadata_ingestor(container_services, s3_client, sqs_client, sts_helper):
    return MetadataIngestor(container_services, s3_client, sqs_client, sts_helper)


@pytest.fixture
def video_ingestor(container_services, s3_client, sqs_client, sts_helper):
    return VideoIngestor(container_services, s3_client, sqs_client, sts_helper)
