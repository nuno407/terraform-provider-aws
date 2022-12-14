"""Tests for metadata.consumer.main module."""
import json
import os
from pathlib import Path
from pymongo.collection import ReturnDocument
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest
from metadata.consumer.main import (
    AWS_REGION,
    create_snapshot_recording_item,
    create_video_recording_item,
    find_and_update_media_references,
    main,
    read_message,
    upsert_data_to_db,
    create_recording_item,
    update_voxel_media,
    upsert_mdf_data,
    process_outputs)
from pytest_mock import MockerFixture  # pylint ignore=wrong-import-order
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS

FIX_RECORDING = {
    "id": "datanauts_DATANAUTS_DEV_01_TrainingRecorder_1669376595000_1669376695538",
    "source_videos": []}

FIX_SNAPSHOT = {
    "id": "honeybadger_rc_srx_develop_cst2hi_01_TrainingMultiSnapshot_" +
    "TrainingMultiSnapshot-a194030a-721a-4ed9-9d59-ebe1c7896a03_5_1669638188317",
    "source_videos": ["honeybadger_rc_srx_develop_cst2hi_01_InteriorRecorder_1669735338000_1669735406429"]}


def _read_test_fixture(fixture_name: str) -> str:
    """Read fixture from test_data directory

    Args:
        fixture_name (str): fixture filename

    Returns:
        str: raw fixture file
    """
    fixture_path = Path(f"{os.path.dirname(__file__)}/test_data/{fixture_name}")
    with open(fixture_path.resolve(), "r", encoding="utf-8") as file_reader:
        return file_reader.read()


def _parsed_message_body_helper(fixture_name: str) -> dict:
    """reads a test fixture and parses into a python dict

    Args:
        fixture_name (str): fixture name located in test_data directory

    Returns:
        dict: parsed message body in dict
    """
    raw_message_body = _read_test_fixture(fixture_name)
    return json.loads(raw_message_body)


def _metadata_sqs_message_helper(body: dict, source_container: str = "SDRetriever") -> dict:
    return {
        "MessageId": "402e519c-2349-4f20-b831-f23013e84dc5",
        "ReceiptHandle": "AQEB4H9hlcjOD70zMjWkO/2+vaVs+wXXX/MLDOx3kKQBpfxS7RlLKjS4zMT2rgWHIQu/" +
        "Kev+ILlxnPPRZm3q9wvYmiMwSYIBtUWEWxfZvesSeNdGWkzw273jMSzqZ3TwH6M1Mc09tG5vWqwIj181RRa0kxe" +
        "JPEQhazNAVdKhJTGsF6tkXf6pj8tFr4RxqNFJlbHVjC9Z4hCmLD905PmXWm5cL2ZRz916Acl0MsWVZB9EUTxGeQf" +
        "u7JjEtSCHRbk2/M3hmRwrmHEPfk9d2Zkw0MZm10TuUT7EIl5jZzSGdZo1cyrHBLghACDj2P4ir6aAQMosaBzkg+" +
        "nlWJEAw0idwO6mGyJk2zq5mTMf6EQ4f+8n3I4s6VYPcdq3TgUQZJBxLMxpaJuUwwk5TezDlNpnglVShrqwHSKgFAImLX7JkI39xyU=",
        "MD5OfBody": "075f577ca76d73f2c14a40312ebbfa34",
        "Body": json.dumps(body),
        "Attributes": {
            "SentTimestamp": "1669383967058",
            "ApproximateReceiveCount": "1"},
        "MD5OfMessageAttributes": "88ff9a8f6c0d1d7bd9414bb7ea9580a3",
        "MessageAttributes": {
            "SourceContainer": {
                "StringValue": source_container,
                "DataType": "String"},
            "ToQueue": {
                "StringValue": "dev-terraform-queue-metadata",
                "DataType": "String"}}}


def _video_message_body(recording_id: str) -> dict:
    return {
        "_id": f"{recording_id}",
        "MDF_available": "No",
        "media_type": "video",
        "s3_path": "dev-rcd-raw-video-files/Debug_Lync/" +
        f"{recording_id}.mp4",
        "footagefrom": 1669376595000,
        "footageto": 1669376695538,
        "tenant": "datanauts",
        "deviceid": "DATANAUTS_DEV_01",
        "length": "0:01:41",
        "#snapshots": "0",
        "snapshots_paths": [],
        "sync_file_ext": "",
        "resolution": "1280x720"}


def _message_attributes_body(source_container: str = "SDRetriever") -> dict:
    return {
        "SourceContainer": {
            "StringValue": source_container,
            "DataType": "String"},
        "ToQueue": {
            "StringValue": "dev-terraform-queue-metadata",
            "DataType": "String"}}


def _expected_video_recording_item(recording_id: str, extension: str = "mp4") -> dict:
    return {
        "MDF_available": "No",
        "_media_type": "video",
        "filepath": f"s3://dev-rcd-raw-video-files/Debug_Lync/{recording_id}.{extension}",
        "recording_overview": {
            "#snapshots": 0,
            "deviceID": "DATANAUTS_DEV_01",
            "length": "0:01:41",
            "snapshots_paths": [],
            "tenantID": "datanauts",
            "time": "2022-11-25 11:43:15"},
        "resolution": "1280x720",
        "video_id": f"{recording_id}"}


def _snapshot_message_body(snapshot_id: str, extension: str = "jpeg") -> dict:
    return {
        "_id": f"{snapshot_id}",
        "s3_path": f"dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}.{extension}",
        "deviceid": "rc_srx_develop_cst2hi_01",
        "timestamp": 1669638188317,
        "tenant": "honeybadger",
        "media_type": "image"}


def _expected_image_recording_item(snapshot_id: str, source_videos: list) -> dict:
    return {
        "_media_type": "image",
        "filepath": f"s3://dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}.jpeg",
        "recording_overview": {
            "deviceID": "rc_srx_develop_cst2hi_01",
            "source_videos": source_videos,
            "tenantID": "honeybadger"
        },
        "video_id": f"{snapshot_id}",
    }


def _snapshot_query(message_id: str) -> dict:
    return {
        "$inc": {"recording_overview.#snapshots": 1},
        "$push": {
            "recording_overview.snapshots_paths": message_id
        }
    }


def _find_and_update_media_references_param(media_paths: list[str], input_query: dict) -> tuple[list[str], dict]:
    return media_paths, input_query


class TestMetadataMain():
    """TestMetadataMain.

    Test functions inside metadata.consumer.main module
    """

    @pytest.fixture
    def mock_update_voxel_media(self, mocker: MockerFixture) -> Mock:
        """mock update_voxel_media

        Args:
            mocker (MockerFixture): mocker for internal dependency

        Returns:
            Mock: a mocked update_voxel_media function
        """
        return mocker.patch("metadata.consumer.main.update_voxel_media")

    @pytest.mark.parametrize("input_media_paths,input_query",
                             [_find_and_update_media_references_param(media_paths=["video1",
                                                                                   "video2"],
                                                                      input_query=_snapshot_query("snapshot1"))])
    @pytest.mark.unit
    def test_find_and_update_media_references(
            self,
            input_media_paths: list[str],
            input_query: dict,
            mock_update_voxel_media: Mock):
        """test_find_and_update_media_references.

        Args:
            input_media_paths (list[str]): media paths from the services.get_related
            input_query (dict): query to find recoding documents and update the references
            mock_update_voxel_media (Mock): mocked function that updates voxel database
        """
        mock_collection = Mock()
        mocked_recording = {"_id": "mocked_recording"}
        mock_collection.find_one_and_update = Mock(return_value=mocked_recording)
        find_and_update_media_references(input_media_paths, input_query, mock_collection)
        mock_collection.find_one_and_update.assert_has_calls(
            [
                call(
                    filter={
                        "video_id": input_media_paths[0]},
                    update=input_query,
                    upsert=False,
                    return_document=ReturnDocument.AFTER),
                call(
                    filter={
                        "video_id": input_media_paths[1]},
                    update=input_query,
                    upsert=False,
                    return_document=ReturnDocument.AFTER)])
        mock_update_voxel_media.assert_has_calls([
            call(mocked_recording),
            call(mocked_recording)
        ])

    @pytest.fixture
    def fixture_find_and_update_media_references(self, mocker: MockerFixture) -> Mock:
        """mocks find_and_update_media_references function
        """
        return mocker.patch("metadata.consumer.main.find_and_update_media_references")

    @pytest.mark.unit
    def test_create_snapshot_recording_item(self, fixture_find_and_update_media_references: Mock):
        """test_create_snapshot_recording_item."""
        given_related_videos = ["test_videoid1", "test_videoid2"]
        given_snapshot_id = "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=given_related_videos)
        mock_collection = Mock()
        mock_collection.find_one_and_update = Mock()
        input_message = _snapshot_message_body(given_snapshot_id)
        snapshot_recording = create_snapshot_recording_item(
            input_message, mock_collection, mock_media_svc)
        expected_recording_item = {
            # we have the key for snapshots named as "video_id" due to legacy reasons...
            "video_id": input_message["_id"],
            "_media_type": input_message["media_type"],
            "filepath": "s3://" + input_message["s3_path"],
            "recording_overview": {
                "tenantID": input_message["tenant"],
                "deviceID": input_message["deviceid"],
                "source_videos": list(given_related_videos)
            }
        }
        assert snapshot_recording == expected_recording_item
        fixture_find_and_update_media_references.assert_called_once_with(
            given_related_videos, update_query={
                "$inc": {
                    "recording_overview.#snapshots": 1}, "$push": {
                    "recording_overview.snapshots_paths": given_snapshot_id}}, recordings_collection=mock_collection)

    @pytest.mark.unit
    def test_create_video_recording_item(self, fixture_find_and_update_media_references: Mock):
        """test_create_video_recording_item"""
        given_related_snapshots = ["test_snapshot1", "test_snapshot2"]
        given_video_id = "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=given_related_snapshots)
        mock_collection = Mock()
        mock_collection.find_one_and_update = Mock()
        input_message = _video_message_body(given_video_id)
        video_recording = create_video_recording_item(input_message, mock_collection, mock_media_svc)
        expected_recording_item: dict = {
            "video_id": input_message["_id"],
            "MDF_available": input_message["MDF_available"],
            "_media_type": input_message["media_type"],
            "filepath": "s3://" + input_message["s3_path"],
            "recording_overview": {
                "tenantID": input_message["tenant"],
                "deviceID": input_message["deviceid"],
                "length": input_message["length"],
                "snapshots_paths": given_related_snapshots,
                "#snapshots": len(given_related_snapshots),
                "time": "2022-11-25 11:43:15"
            },
            "resolution": input_message["resolution"]
        }
        assert video_recording == expected_recording_item
        fixture_find_and_update_media_references.assert_called_once_with(
            given_related_snapshots,
            update_query={
                "$push": {
                    "recording_overview.source_videos": input_message["_id"]}},
            recordings_collection=mock_collection)

    @pytest.mark.parametrize("message_body,source_videos,expected_outcome", [
        (_snapshot_message_body(FIX_SNAPSHOT["id"]), FIX_SNAPSHOT["source_videos"],  # type: ignore
         _expected_image_recording_item(FIX_SNAPSHOT["id"], FIX_SNAPSHOT["source_videos"])),  # type: ignore
        (_video_message_body(FIX_RECORDING["id"]), FIX_RECORDING["source_videos"],  # type: ignore
         _expected_video_recording_item(FIX_RECORDING["id"]))   # type: ignore
    ])
    @pytest.mark.unit
    def test_create_recording_item(self, message_body, source_videos, expected_outcome,
                                   fixture_find_and_update_media_references: Mock):
        mock_collection = Mock()
        mock_collection.find_one_and_update = Mock()
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=source_videos)

        obtained_outcome = create_recording_item(message_body, mock_collection, mock_media_svc)

        assert obtained_outcome == expected_outcome
        fixture_find_and_update_media_references.assert_called()
        mock_collection.find_one_and_update.assert_called_once_with(
            filter={"video_id": message_body["_id"]},
            update={"$set": obtained_outcome},
            upsert=True, return_document=ReturnDocument.AFTER
        )

    @pytest.mark.parametrize("file_format,anonymized_path,voxel_dataset_name",
                             [*[(file_format,
                                 f"s3://anon_bucket/b/c/d_anonymized.{file_format}",
                                 "b_snapshots") for file_format in IMAGE_FORMATS],
                              *[(file_format,
                                 f"s3://anon_bucket/b/c/d_anonymized.{file_format}",
                                 "b") for file_format in VIDEO_FORMATS],
                              ])
    @pytest.mark.unit
    @patch("metadata.consumer.main.update_sample")
    @patch("metadata.consumer.main.create_dataset")
    @patch.dict("metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket"})
    def test_update_voxel_media(
            self,
            mock_create_dataset_voxel: Mock,
            mock_update_sample_voxel: Mock,
            file_format: dict,
            anonymized_path: str,
            voxel_dataset_name: str,):
        # Given
        recording_item: dict = {
            "_id": "test",
            "filepath": f"s3://a/b/c/d.{file_format}"
        }
        sample = recording_item.copy()
        sample.pop("_id")
        sample["s3_path"] = anonymized_path

        # When
        update_voxel_media(recording_item)

        # Then
        mock_create_dataset_voxel.assert_called_once_with(voxel_dataset_name)
        mock_update_sample_voxel.assert_called_once_with(voxel_dataset_name, sample)

    @patch("metadata.consumer.main.ContainerServices.download_file",
           return_value=json.dumps({"a": "b"}).encode("UTF-8"))
    @pytest.mark.unit
    def test_upsert_mdf_data(self, _: Mock):
        # Given
        mock_collection_rec = Mock()
        mock_collection_rec.find_one_and_update = Mock()
        mock_collection_sig = Mock()
        mock_collection_sig.update_one = Mock()
        message = json.loads(_read_test_fixture("input_raw_message_body_metadata_mdfparser"))

        # When
        recording = upsert_mdf_data(message, mock_collection_sig, mock_collection_rec)

        # Then
        assert recording is not None
        mock_collection_rec.find_one_and_update.assert_called_once_with(
            filter={
                "video_id": "honeybadger_rc_srx_develop_cst2hi_01_InteriorRecorder_1669638678819_1669638709438"},
            update={
                "$set": {
                    "recording_overview.number_chc_events": 1,
                    "recording_overview.chc_duration": 18.956827,
                    "recording_overview.max_person_count": 1.0,
                    "recording_overview.ride_detection_counter": 0}},
            upsert=True,
            return_document=True)
        mock_collection_sig.update_one.assert_called_once_with(
            filter={
                "recording": "honeybadger_rc_srx_develop_cst2hi_01_InteriorRecorder_1669638678819_1669638709438",
                "source": {
                    "$regex": "MDF.*"}},
            update={
                "$set": {
                    "source": "MDFParser",
                    "signals": {
                        "a": "b"}}},
            upsert=True)

    @patch("metadata.consumer.main.update_sample")
    @patch("metadata.consumer.main.create_dataset")
    @patch("metadata.consumer.main.download_and_synchronize_chc", return_value=({"a": "b"}, {"c": "d"}))
    @patch.dict("metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket"})
    @pytest.mark.unit
    def test_process_outputs(
            self,
            mock_download_and_synchronize_chc: Mock,
            mock_create_dataset_voxel: Mock,
            mock_update_sample_voxel: Mock,):
        # Given
        video_id = "id"
        message = {
            "s3_path": f"a/b/c.d",
            "output": {
                "bucket": "a",
                "meta_path": "e/f.media"
            }
        }
        collection_algo_out = Mock()
        collection_algo_out.update_one = Mock()
        collection_recordings = Mock()
        collection_signals = Mock()
        collection_signals.update_one = Mock()
        source = "CHC"
        expected_out = {
            "_id": "id_CHC",
            "algorithm_id": "CHC",
            "pipeline_id": "id",
            "output_paths": {
                "metadata": "a/e/f.media"
            },
            "algorithms": {
                "id_CHC": {
                    "results": {
                        "CHBs_sync": {
                            "a": "b"
                        },
                        "CHC_metrics": {
                            "c": "d"
                        }
                    },
                    "output_paths": {
                        "metadata": "a/e/f.media"
                    }
                }
            },
            "s3_path": "s3://anon_bucket/a/b/c_anonymized.d",
            "video_id": "id"
        }

        # When
        process_outputs(video_id, message, collection_algo_out, collection_recordings, collection_signals, source)

        # Then
        collection_signals.update_one.assert_called_once_with(
            {
                "recording": "id", "algo_out_id": "id_CHC"}, {
                "$set": {
                    "algo_out_id": "id_CHC",
                    "recording": "id",
                    "source": "CHC",
                    "signals": {"a": "b"}
                }
            },
            upsert=True
        )
        collection_algo_out.update_one.assert_called_once_with(
            {'_id': 'id_CHC'},
            {
                "$set": expected_out
            }, upsert=True)
        mock_download_and_synchronize_chc.assert_called_once_with("id", collection_recordings, "a", "e/f.media")
        mock_create_dataset_voxel.assert_called_once_with("a")
        mock_update_sample_voxel.assert_called_once_with("a", expected_out)

    @pytest.mark.unit
    @pytest.mark.parametrize("input_message_body,input_message_atributes,return_upserts", [
        (
            _snapshot_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore
            _message_attributes_body(),  # type: ignore
            True
        ),
        (
            _snapshot_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore
            _message_attributes_body(),  # type: ignore
            False
        ),
        (
            _snapshot_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore
            _message_attributes_body("MDFParser"),  # type: ignore
            True
        ),
        (
            _snapshot_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore
            _message_attributes_body("MDFParser"),  # type: ignore
            False
        ),
    ])
    @patch("metadata.consumer.main.create_recording_item")
    @patch("metadata.consumer.main.upsert_mdf_data")
    @patch("metadata.consumer.main.update_voxel_media")
    def test_upsert_data_to_db_sdr_mdf(
            self,
            update_voxel_media: Mock,
            upsert_mdf_data: Mock,
            create_recording_item: Mock,
            input_message_body: dict,
            input_message_atributes: dict,
            return_upserts: bool
    ):

        # GIVEN
        db_mock = Mock()
        collection_recordings_mock = Mock()
        collection_signals_mock = Mock()
        container_services_mock = Mock()
        container_services_mock.db_tables = MagicMock()
        create_recording_item.return_value = return_upserts
        upsert_mdf_data.return_value = return_upserts
        db_mock.__getitem__ = Mock(
            side_effect=[
                collection_signals_mock,
                collection_recordings_mock,
                "pipeline_exec",
                "algo_output"])
        related_metadata_service_mock = Mock()

        # THEN
        upsert_data_to_db(
            db_mock,
            container_services_mock,
            related_metadata_service_mock,
            input_message_body,
            input_message_atributes)

        source = input_message_atributes["SourceContainer"]["StringValue"]
        if source == "SDRetriever":
            create_recording_item.assert_called_once_with(
                input_message_body,
                collection_recordings_mock,
                related_metadata_service_mock)
        else:
            upsert_mdf_data.assert_called_once_with(input_message_body,
                                                    collection_signals_mock,
                                                    collection_recordings_mock)

        if return_upserts:
            update_voxel_media.assert_called_once_with(return_upserts)
        else:
            update_voxel_media.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize("input_message_body,input_message_atributes", [
    ])
    @patch("metadata.consumer.main.update_pipeline_db")
    @patch("metadata.consumer.main.process_outputs")
    def test_upsert_data_to_db(
            self,
            process_outputs: Mock,
            update_pipeline_db: Mock,
            input_message_body: dict,
            input_message_atributes: dict,
    ):

        # GIVEN
        db_mock = Mock()
        collection_recordings_mock = Mock()
        collection_signals_mock = Mock()
        collection_algo_output_mock = Mock()
        collection_pipeline_exec_mock = Mock()
        collection_signals_mock = Mock()
        container_services_mock = Mock()
        container_services_mock.db_tables = MagicMock()
        recording_id = os.path.basename(input_message_body["s3_path"]).split(".")[0]
        db_mock.__getitem__ = Mock(
            side_effect=[
                collection_signals_mock,
                collection_recordings_mock,
                collection_pipeline_exec_mock,
                collection_algo_output_mock])
        related_metadata_service_mock = Mock()

        # THEN
        upsert_data_to_db(
            db_mock,
            container_services_mock,
            related_metadata_service_mock,
            input_message_body,
            input_message_atributes)

        source = input_message_atributes["SourceContainer"]["StringValue"]
        update_pipeline_db.assert_called_once_with(
            recording_id,
            input_message_body,
            collection_pipeline_exec_mock,
            source)

        if "output" in input_message_body:
            process_outputs.assert_called_once_with(
                recording_id,
                input_message_body,
                collection_algo_output_mock,
                collection_recordings_mock,
                collection_signals_mock,
                source)
        else:
            process_outputs.assert_not_called()

    read_message_test_data: dict = {
        "mdf": {
            "input": _read_test_fixture("input_raw_message_body_metadata_mdfparser"),
            "expected": _parsed_message_body_helper("expected_parsed_message_body_metadata_mdfparser.json")
        }
    }

    @pytest.mark.parametrize("raw_message,expected_message", [
        (
            read_message_test_data.get("mdf").get("input"),   # type: ignore
            read_message_test_data.get("mdf").get("expected")  # type: ignore
        ),
    ])
    @pytest.mark.unit
    def test_read_message(self, raw_message: str, expected_message: dict):
        """test read_message body

        Args:
            raw_message (str): input raw message body
            expected_message (dict): expected parsed message body
        """
        container_services_mock = Mock()
        container_services_mock.display_processed_msg = Mock()
        got_relay_data = read_message(container_services_mock, raw_message)

        assert got_relay_data == expected_message
        container_services_mock.display_processed_msg.assert_called_once()

    @pytest.fixture
    def mock_boto3_client(self, mocker: MockerFixture) -> Mock:
        """Mock boto3 AWS client."""
        return mocker.patch("boto3.client")

    @pytest.fixture
    def mock_persistence(self, mocker: MockerFixture) -> Mock:
        """Mock Persistence class."""
        return mocker.patch("metadata.consumer.main.Persistence")

    @pytest.fixture
    def mock_related_media_service(self, mocker: MockerFixture) -> Mock:
        """Mock RelatedMediaservice class."""
        return mocker.patch("metadata.consumer.main.RelatedMediaService")

    @pytest.fixture
    def mock_base(self, mocker: MockerFixture) -> Mock:
        """Mock base package."""
        return mocker.patch("base")

    @pytest.fixture
    def mock_graceful_exit(self, mocker: MockerFixture) -> Mock:
        """Mock GracefulExit class."""
        return mocker.patch("metadata.consumer.main.GracefulExit")

    @pytest.fixture
    def mock_read_message(self, mocker: MockerFixture) -> Mock:
        """Mock read_message function."""
        return mocker.patch("metadata.consumer.main.read_message")

    @pytest.fixture
    def mock_upsert_data_to_db(self, mocker: MockerFixture) -> Mock:
        """Mock upsert_data_to_db function."""
        return mocker.patch("metadata.consumer.main.upsert_data_to_db")

    @pytest.fixture
    def mock_container_services(self, mocker: MockerFixture) -> Mock:
        """Mock ContainerServices class."""
        return mocker.patch("metadata.consumer.main.ContainerServices")

    @pytest.fixture
    def mock_continue_running(self, mocker: MockerFixture) -> Mock:
        """Mock GracefulExit.continue_running property."""
        return mocker.patch("metadata.consumer.main.GracefulExit.continue_running",
                            new_callable=PropertyMock, side_effect=[True, False])

    @pytest.mark.unit
    def test_metadata_consumer_main(
            self,
            mock_container_services: Mock,
            mock_boto3_client: Mock,
            mock_persistence: Mock,
            mock_related_media_service: Mock,
            mock_read_message: Mock,
            mock_upsert_data_to_db: Mock,
            mock_continue_running: Mock):
        """Test metadata.consumer.main

        Args:
            mock_container_services (Mock): mocked container services object
            mock_boto3_client (Mock): mocked boto3 client
            mock_persistence (Mock): mocked persistence object
            mock_related_media_service (Mock): mocked media service object
            mock_read_message (Mock): mocked read_message function
            mock_upsert_data_to_db (Mock): mocked upser_data_to_db function
            mock_continue_running (Mock): mocked continue_running property from GracefulExit
        """
        input_message = _metadata_sqs_message_helper({
            "dummy_body": "dummy_body_value",
        })
        s3_client_mock = Mock()
        sqs_client_mock = Mock()
        mock_boto3_client.side_effect = [s3_client_mock, sqs_client_mock]
        mock_container_services_object = Mock()
        mock_container_services.return_value = mock_container_services_object
        mock_container_services_object.load_config_vars = Mock()
        mock_db_client = Mock()
        mock_db_client.client = Mock()
        mock_container_services_object.create_db_client = Mock(return_value=mock_db_client)
        mock_container_services_object.db_tables = Mock()
        mock_container_services_object.delete_message = Mock()
        mock_container_services_object.listen_to_input_queue = Mock(return_value=input_message)
        mock_persistence_object = Mock()
        mock_persistence.return_value = mock_persistence_object
        mock_api_service = Mock()
        mock_related_media_service.return_value = mock_api_service
        mock_relay_list = Mock()
        mock_read_message.return_value = mock_relay_list

        main()

        mock_boto3_client.assert_any_call("s3", region_name=AWS_REGION)
        mock_boto3_client.assert_any_call("sqs", region_name=AWS_REGION)
        mock_container_services_object.load_config_vars.assert_called_once_with(s3_client_mock)
        mock_container_services_object.create_db_client.assert_called_once_with()
        mock_persistence.assert_called_once_with(None, mock_container_services_object.db_tables, mock_db_client.client)
        mock_related_media_service.assert_called_once_with(mock_persistence_object)
        mock_container_services_object.listen_to_input_queue.assert_called_once_with(sqs_client_mock)
        mock_read_message.assert_called_once_with(mock_container_services_object, input_message["Body"])
        mock_upsert_data_to_db.assert_called_once_with(
            mock_db_client,
            mock_container_services_object,
            mock_api_service,
            mock_relay_list,
            input_message["MessageAttributes"])
        mock_container_services_object.delete_message.assert_called_once_with(
            sqs_client_mock, input_message["ReceiptHandle"])
