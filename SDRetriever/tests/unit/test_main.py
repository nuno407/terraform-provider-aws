import json
import os
from copy import deepcopy
from unittest.mock import ANY, MagicMock, Mock, patch, call

import pytest
from sdretriever.main import (FRONT_RECORDER, INTERIOR_RECORDER,
                              METADATA_FILE_EXT, SNAPSHOT, TRAINING_RECORDER,
                              IngestorHandler, SDRetrieverConfig)
from sdretriever.message import SnapshotMessage, VideoMessage


@pytest.mark.unit
@pytest.mark.usefixtures(
    "container_services",
    "sqs_client",
    "config_yml",
    "get_raw_sqs_message",
    "training_message_metadata",
    "message_metadata")
def get_raw_sqs_message(sqs_message_file_name) -> dict:
    data = None
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/rcc_messages_raw/{sqs_message_file_name}", "r") as f:
        data = json.load(f)
    return data


def result_ingestor(resolution: str = "640x360") -> dict:
    db_record_data = {
        "_id": "deepsensation_rc_srx_develop_ivs1hi_04_InteriorRecorder_1639126766137_1639126800000",
        "MDF_available": "No",
        "media_type": "video",
        "s3_path": "s3://dev-rcd-raw-video-files/Debug_Lync/deepsensation_rc_srx_develop_ivs1hi_04_InteriorRecorder_1639126766137_1639126800000.mp4",
        "footagefrom": 1678105669000,
        "footageto": 1678106669000,
        "tenant": "datanauts",
        "deviceid": "rc_srx_develop_ivs1hi_04",
        "length": "0:00:34",
        "#snapshots": str(0),
        "snapshots_paths": [],
        "sync_file_ext": "",
        "resolution": resolution,
        "internal_message_reference_id": "kjsdhgiuryg78erguhdkg",
    }
    if resolution == "1280x768":
        db_record_data["_id"] = db_record_data["_id"].replace("InteriorRecorder", "TrainingRecorder")
        db_record_data["s3_path"] = db_record_data["s3_path"].replace("InteriorRecorder", "TrainingRecorder")

    return db_record_data


class TestMain:

    @pytest.fixture
    def ing_handler(self, config_yml: SDRetrieverConfig, container_services, sqs_client) -> IngestorHandler:
        return IngestorHandler(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            container_services,
            config_yml,
            sqs_client)

    @pytest.mark.unit
    @pytest.mark.parametrize("message,expected", [
        (
            {"FrontRecorder": "test"},
            FRONT_RECORDER
        ),
        (
            {"Nothing": "test"},
            None
        ),
        (
            get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
            INTERIOR_RECORDER
        ),
        (
            get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
            TRAINING_RECORDER
        ),
        (
            get_raw_sqs_message("TrainingMultiSnapshot_Selector_SQS.json"),
            SNAPSHOT
        )
    ])
    def test_message_type_identifier(self, message: dict, expected: str):
        result = IngestorHandler.message_type_identifier(message)
        assert result == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("message,is_valid,is_irrelavante,source,result", [
        (
            pytest.lazy_fixture("message_metadata"),
            True,
            False,
            "selector",
            True,
        ),
        (
            pytest.lazy_fixture("message_metadata"),
            False,
            False,
            "download",
            False,
        ),
        (
            pytest.lazy_fixture("training_message_metadata"),
            True,
            True,
            "download",
            False,
        ),
    ])
    def test_message_ingestable(
        self,
        ing_handler: IngestorHandler,
        message: VideoMessage,
        is_valid: bool,
        is_irrelavante: bool,
        source: str,
        result: bool
    ):

        message.validate = Mock(return_value=is_valid)
        message.is_irrelevant = Mock(return_value=is_irrelavante)

        result_test = ing_handler.message_ingestable(message, source)

        message.validate.assert_called_once()

        if is_valid:
            message.is_irrelevant.assert_called_once()

        if not is_valid or is_irrelavante:
            ing_handler.cont_services.delete_message.assert_called_once_with(
                ing_handler.sqs_client, message.receipthandle, source)

        assert result == result_test

    @pytest.mark.unit
    @patch("sdretriever.main.VideoMessage")
    @pytest.mark.parametrize(
        "message,is_ingestable,result_check_all_parts,result_ingest,is_metadata_ingested,source",
        [
            (
                get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                result_ingestor(),
                True,
                "download"
            ),
            (
                get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
                False,
                (True, {"some_path"}),
                result_ingestor(),
                True,
                "download"
            ),
            (
                get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
                True,
                (False, {}),
                result_ingestor(),
                True,
                "download"
            ),
            (
                get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                result_ingestor(),
                False,
                "download"
            ),
            (
                get_raw_sqs_message("InteriorRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                {},
                True,
                "download"
            )

        ]
    )
    def test_interior_recorder(
        self,
        video_message_mock: Mock,
        ing_handler: IngestorHandler,
        message: dict,
        is_ingestable: bool,
        result_check_all_parts: tuple[bool, list[str]],
        result_ingest: dict,
        is_metadata_ingested: bool,
        source: str,
    ):
        video_message = VideoMessage(message)
        video_message_mock.return_value = video_message

        ing_handler.message_ingestable = Mock(return_value=is_ingestable)
        ing_handler.metadata_ing.check_allparts_exist = Mock(return_value=result_check_all_parts)
        ing_handler.metadata_ing.ingest = Mock(return_value=is_metadata_ingested)
        ing_handler.video_ing.ingest = Mock(return_value=deepcopy(result_ingest))

        # Function to test
        ing_handler.handle_interior_recorder(message, source)

        # Assert is valid
        ing_handler.message_ingestable.assert_called_once_with(video_message, source)
        if not is_ingestable:
            return

        # Assert all parts exist
        ing_handler.metadata_ing.check_allparts_exist.assert_called_once_with(
            video_message)
        if not result_check_all_parts[0]:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        # Assert video is ingested
        ing_handler.video_ing.ingest.assert_called_once_with(
            video_message, ing_handler.config.training_whitelist, ing_handler.config.request_training_upload)
        if not result_ingest:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        # Assert metadata is ingested
        ing_handler.metadata_ing.ingest.assert_called_once_with(
            video_message, result_ingest['_id'], result_check_all_parts[1])
        if not is_metadata_ingested:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        result_ingest.update({"MDF_available": "Yes", "sync_file_ext": METADATA_FILE_EXT})
        ing_handler.cont_services.send_message.assert_has_calls([
            call(ing_handler.sqs_client, ing_handler.hq_request_queue, ANY),
            call(ing_handler.sqs_client, ing_handler.metadata_queue, result_ingest)
        ])
        ing_handler.cont_services.delete_message.assert_called_once_with(
            ing_handler.sqs_client, video_message.receipthandle, source)

    @pytest.mark.unit
    @patch("sdretriever.main.VideoMessage")
    @pytest.mark.parametrize(
        "message,is_ingestable,result_check_all_parts,result_ingest,imu_path,source",
        [
            (
                get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                result_ingestor("1280x768"),
                "some/path/to/imu",
                "download"
            ),
            (
                get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
                False,
                (True, {"some_path"}),
                result_ingestor("1280x768"),
                "some/path/to/imu",
                "download"
            ),
            (
                get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
                True,
                (False, {}),
                result_ingestor("1280x768"),
                "some/path/to/imu",
                "download"
            ),
            (
                get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                result_ingestor("1280x768"),
                None,
                "download"
            ),
            (
                get_raw_sqs_message("TrainingRecorder_Download_SQS.json"),
                True,
                (True, {"some_path"}),
                {},
                True,
                "download"
            )

        ]
    )
    def test_training_recorder(
        self,
        video_message_mock: Mock,
        ing_handler: IngestorHandler,
        message: dict,
        is_ingestable: bool,
        result_check_all_parts: tuple[bool, list[str]],
        result_ingest: dict,
        imu_path: str,
        source: str,
    ):
        video_message = VideoMessage(message)
        video_message_mock.return_value = video_message

        ing_handler.message_ingestable = Mock(return_value=is_ingestable)
        ing_handler.imu_ing.check_allparts_exist = Mock(return_value=result_check_all_parts)
        ing_handler.imu_ing.ingest = Mock(return_value=imu_path)
        ing_handler.video_ing.ingest = Mock(return_value=deepcopy(result_ingest))

        # Function to test
        ing_handler.handle_training_recorder(message, source)

        # Assert is valid
        ing_handler.message_ingestable.assert_called_once_with(video_message, source)
        if not is_ingestable:
            return

        # Assert all parts exist
        ing_handler.imu_ing.check_allparts_exist.assert_called_once_with(
            video_message)
        if not result_check_all_parts[0]:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        # Assert video is ingested
        ing_handler.video_ing.ingest.assert_called_once_with(
            video_message, ing_handler.config.training_whitelist, ing_handler.config.request_training_upload)
        if not result_ingest:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        # Assert metadata is ingested
        ing_handler.imu_ing.ingest.assert_called_once_with(
            video_message, result_ingest['_id'], result_check_all_parts[1])
        if not imu_path:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, video_message.receipthandle, ANY, source)
            return

        result_ingest.update({"imu_path": imu_path})
        ing_handler.cont_services.send_message.assert_called_once_with(
            ing_handler.sqs_client, ing_handler.metadata_queue, result_ingest)
        ing_handler.cont_services.delete_message.assert_called_once_with(
            ing_handler.sqs_client, video_message.receipthandle, source)

    @pytest.mark.unit
    @patch("sdretriever.main.SnapshotMessage")
    @pytest.mark.parametrize(
        "message,is_valid,is_irrelevant,result_ingest",
        [
            (
                get_raw_sqs_message("TrainingMultiSnapshot_Selector_SQS.json"),
                True,
                (True, {"some_path"}),
                result_ingestor("1280x768"),
            ),
            (
                get_raw_sqs_message("TrainingMultiSnapshot_Selector_SQS.json"),
                False,
                (True, {"some_path"}),
                result_ingestor("1280x768"),
            ),
            (
                get_raw_sqs_message("TrainingMultiSnapshot_Selector_SQS.json"),
                True,
                (False, {}),
                result_ingestor("1280x768"),
            ),

        ]
    )
    def test_training_snapshot(
        self,
        snapshot_message_mock: Mock,
        ing_handler: IngestorHandler,
        message: dict,
        is_valid: bool,
        is_irrelevant: bool,
        result_ingest: dict
    ):

        source = "selector"
        snapshot_message = SnapshotMessage(message)
        snapshot_message_mock.return_value = snapshot_message

        snapshot_message.validate = Mock(return_value=is_valid)
        snapshot_message.is_irrelevant = Mock(return_value=is_irrelevant)
        ing_handler.snap_ing.ingest = Mock(return_value=result_ingest)

        # Function to test
        ing_handler.handle_snapshot(message, source)

        # Assert is valid
        snapshot_message.validate.assert_called_once()
        if not is_valid:
            ing_handler.cont_services.delete_message.assert_called_once_with(
                ing_handler.sqs_client, snapshot_message.receipthandle, source)
            return

        # Assert all parts exist
        snapshot_message.is_irrelevant.assert_called_once_with(ing_handler.config.tenant_blacklist)
        if is_irrelevant:
            ing_handler.cont_services.delete_message.assert_called_once_with(
                ing_handler.sqs_client, snapshot_message.receipthandle, source)
            return

        # Assert snapshot is ingested
        ing_handler.snap_ing.ingest.assert_called_once_with(snapshot_message)
        if not result_ingest:
            ing_handler.cont_services.update_message_visibility.assert_called_once_with(
                ing_handler.sqs_client, snapshot_message.receipthandle, ANY, source)
