"""Tests for metadata.consumer.main module."""
# pylint: disable=missing-function-docstring,missing-module-docstring
import copy
import hashlib
import json
import os
from mongoengine import connect, disconnect
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import ANY, MagicMock, Mock, PropertyMock, call, patch

import pytest
from botocore.errorfactory import ClientError
from pymongo.collection import ReturnDocument
from pymongo.errors import DocumentTooLarge
from pytest_mock import MockerFixture
from pytz import UTC
from base.testing.utils import get_abs_path
from base.aws.sqs import parse_message_body_to_dict

import metadata.consumer.main as main
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
from base.model.event_types import Location
from metadata.consumer.bootstrap import bootstrap_di
from kink import di


from metadata.consumer.main import (AWS_REGION, MetadataCollections,
                                    create_recording_item,
                                    create_snapshot_recording_item,
                                    create_video_recording_item,
                                    find_and_update_media_references,
                                    fix_message,
                                    parse_message_body_to_dict, process_outputs,
                                    process_sanitizer,
                                    transform_data_to_update_query,
                                    update_voxel_media, upsert_data_to_db,
                                    upsert_mdf_signals_data)

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
    fixture_path = Path(
        f"{os.path.dirname(__file__)}/test_data/{fixture_name}")
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


def _video_message_body(recording_id: str, imu_path: Optional[str] = None) -> dict:
    result = {
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
        "recording_duration": 101.0,
        "#snapshots": "0",
        "snapshots_paths": [],
        "sync_file_ext": "",
        "devcloudid": hashlib.sha256("Dummy_data".encode("utf-8")).hexdigest(),
        "resolution": "1280x720"}

    if imu_path is not None:
        result["imu_path"] = imu_path

    # read_message function fix some issues on message structure
    return fix_message(Mock(), str(result), result)


def _video_message_dict(recording_id: str, imu_path: Optional[str] = None) -> dict:
    result = {
        "video_id": recording_id,
        "MDF_available": "No",
        "_media_type": "video",
        "filepath": "s3://" + "dev-rcd-raw-video-files/Debug_Lync/" +
        f"{recording_id}.mp4",
        "recording_overview": {
            "tenantID": "datanauts",
            "deviceID": "DATANAUTS_DEV_01",
            "length": "0:01:41",
            "recording_duration": 101.0,
            "snapshots_paths": ["test_snapshot1", "test_snapshot2"],
            "#snapshots": 2,
            "time": "2022-11-25 11:43:15",
            "recording_time": datetime(2022, 11, 25, 11, 43, 15, tzinfo=timezone.utc),
            "devcloudid": hashlib.sha256("Dummy_data".encode("utf-8")).hexdigest()
        },
        "resolution": "1280x720"
    }

    if imu_path is not None:
        result["recording_overview"]["imu_path"] = imu_path

    return result


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
            "recording_duration": 101.0,
            "snapshots_paths": [],
            "tenantID": "datanauts",
            "time": "2022-11-25 11:43:15",
            "recording_time": datetime(2022, 11, 25, 11, 43, 15, tzinfo=timezone.utc),
            "devcloudid": hashlib.sha256("Dummy_data".encode("utf-8")).hexdigest(),
        },
        "resolution": "1280x720",
        "video_id": f"{recording_id}"}


def _snapshot_message_body(snapshot_id: str, extension: str = "jpeg") -> dict:
    result = {
        "_id": f"{snapshot_id}",
        "s3_path": f"dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}.{extension}",
        "deviceid": "rc_srx_develop_cst2hi_01",
        "timestamp": 1669638188317,
        "tenant": "honeybadger",
        "media_type": "image",
        "referred_artifact": {
            "tenant_id": "honeybadger",
            "device_id": "rc_srx_develop_cst2hi_01",

        },
        "devcloudid": hashlib.sha256(snapshot_id.encode("utf-8")).hexdigest()
    }

    # read_message function fix some issues on message structure
    return fix_message(Mock(), str(result), result)


def _snapshot_sdr_message_body(snapshot_id: str, extension: str = "jpeg") -> dict:
    return {
        "_id": f"{snapshot_id}",
        "s3_path": f"dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}.{extension}",
        "deviceid": "rc_srx_develop_cst2hi_01",
        "timestamp": 1669638188317,
        "metadata_path": f"s3://dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}_metadata.json",
        "tenant": "honeybadger",
        "media_type": "image",
        "internal_message_reference_id": hashlib.sha256(snapshot_id.encode("utf-8")).hexdigest()
    }


def _mdf_imu_message_body(_id: str) -> dict:
    return {
        "_id": _id,
        "parsed_file_path": f"s3://dev-rcd-raw-video-files/Debug_Lync/{_id}_signals.json",
        "data_type": "metadata",
        "recording_overview": {}
    }


def _mdf_metadata_message_body(_id: str) -> dict:
    return {
        "_id": _id,
        "parsed_file_path": f"s3://dev-rcd-raw-video-files/Debug_Lync/{_id}_signals.json",
        "data_type": "metadata",
        "recording_overview": {"number_chc_events": 1,
                               "ride_detection_people_count_before": 1,
                               "ride_detection_people_count_after": 0,
                               "sum_door_closed": 0,
                               "variance_person_count": 0.04}
    }


def _expected_image_recording_item(snapshot_id: str, source_videos: list) -> dict:
    return {
        "_media_type": "image",
        "filepath": f"s3://dev-rcd-raw-video-files/Debug_Lync/{snapshot_id}.jpeg",
        "recording_overview": {
            "deviceID": "rc_srx_develop_cst2hi_01",
            "source_videos": source_videos,
            "tenantID": "honeybadger",
            "devcloudid": hashlib.sha256(snapshot_id.encode("utf-8")).hexdigest(),
            "recording_time": datetime(2022, 11, 28, 12, 23, 8, 317000, tzinfo=timezone.utc)
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


class TestMetadataMain():  # pylint: disable=too-many-public-methods
    """TestMetadataMain.

    Test functions inside metadata.consumer.main module
    """

    @pytest.fixture(name="mock_update_voxel_media")
    def fixture_update_voxel_media(self, mocker: MockerFixture) -> Mock:
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
        mock_collection.find_one_and_update = Mock(
            return_value=mocked_recording)
        find_and_update_media_references(
            input_media_paths, input_query, mock_collection)
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

    @pytest.fixture(name="mock_find_and_update_media_references")
    def fixture_find_and_update_media_references(self, mocker: MockerFixture) -> Mock:
        """mocks find_and_update_media_references function
        """
        return mocker.patch("metadata.consumer.main.find_and_update_media_references")

    @pytest.mark.unit
    def test_create_snapshot_recording_item(self, mock_find_and_update_media_references: Mock):
        """test_create_snapshot_recording_item."""
        given_related_videos = ["test_videoid1", "test_videoid2"]
        given_snapshot_id = "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"  # pylint: disable=line-too-long
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=given_related_videos)
        mock_collection = Mock()
        mock_collection.find_one = Mock(return_value=None)
        mock_collection.find_one_and_update = Mock()
        input_message = _snapshot_message_body(given_snapshot_id)
        snapshot_recording = create_snapshot_recording_item(
            input_message, mock_collection, mock_media_svc)
        expected_recording_item = {
            # we have the key for snapshots named as "video_id" due to legacy reasons...
            "video_id": input_message["_id"],
            "_media_type": input_message["media_type"],
            "filepath": input_message["s3_path"],
            "recording_overview": {
                "tenantID": input_message["tenant"],
                "deviceID": input_message["deviceid"],
                "source_videos": list(given_related_videos),
                "devcloudid": input_message["devcloudid"],
                "recording_time": datetime(2022, 11, 28, 12, 23, 8, 317000, tzinfo=timezone.utc)
            }
        }
        assert snapshot_recording == expected_recording_item
        mock_find_and_update_media_references.assert_called_once_with(
            given_related_videos, update_query={
                "$inc": {
                    "recording_overview.#snapshots": 1}, "$push": {
                    "recording_overview.snapshots_paths": given_snapshot_id}}, recordings_collection=mock_collection)

    @pytest.mark.unit
    def test_create_snapshot_recording_item_duplicated(self, mock_find_and_update_media_references: Mock):
        """test_create_snapshot_recording_item."""
        given_related_videos = ["test_videoid1", "test_videoid2"]
        given_snapshot_id = "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"  # pylint: disable=line-too-long
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=given_related_videos)
        mock_collection = Mock()
        mock_collection.find_one = Mock(return_value="mocked_value")
        mock_collection.find_one_and_update = Mock()
        input_message = _snapshot_message_body(given_snapshot_id)
        snapshot_recording = create_snapshot_recording_item(
            input_message, mock_collection, mock_media_svc)
        expected_recording_item = {
            # we have the key for snapshots named as "video_id" due to legacy reasons...
            "video_id": input_message["_id"],
            "_media_type": input_message["media_type"],
            "filepath": input_message["s3_path"],
            "recording_overview": {
                "tenantID": input_message["tenant"],
                "deviceID": input_message["deviceid"],
                "source_videos": list(given_related_videos),
                "devcloudid": input_message["devcloudid"],
                "recording_time": datetime(2022, 11, 28, 12, 23, 8, 317000, tzinfo=timezone.utc)
            }
        }
        assert snapshot_recording == expected_recording_item
        mock_find_and_update_media_references.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_message,expected_recording_item",
        [(_video_message_body(
            "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),
          _video_message_dict(
            "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044")),
         (_video_message_body(
             "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044",
             "imu/path/data.csv"),
          _video_message_dict(
             "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044",
             "imu/path/data.csv"))])
    def test_create_video_recording_item(
            self,
            mock_find_and_update_media_references: Mock,
            input_message: dict,
            expected_recording_item: dict):
        """test_create_video_recording_item"""

        # GIVEN
        given_related_snapshots = ["test_snapshot1", "test_snapshot2"]
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=given_related_snapshots)
        mock_collection = Mock()
        mock_collection.find_one_and_update = Mock()

        # WHEN
        video_recording = create_video_recording_item(input_message, mock_collection, mock_media_svc)

        # THEN
        assert video_recording == expected_recording_item
        mock_find_and_update_media_references.assert_called_once_with(
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
                                   mock_find_and_update_media_references: Mock):
        mock_collection = Mock()
        mock_collection.find_one = Mock(return_value=None)
        mock_collection.find_one_and_update = Mock()
        mock_media_svc = Mock()
        mock_media_svc.get_related = Mock(return_value=source_videos)

        obtained_outcome = create_recording_item(
            message_body, mock_collection, mock_media_svc)

        assert obtained_outcome == expected_outcome
        mock_find_and_update_media_references.assert_called()
        mock_collection.find_one_and_update.assert_called_once_with(
            filter={"video_id": message_body["_id"]},
            update={"$set": transform_data_to_update_query(obtained_outcome)},
            upsert=True, return_document=ReturnDocument.AFTER
        )

    @pytest.mark.unit
    def test_create_event(self):
        # GIVEN
        device_id = "test_device_id"
        tenant_id = "test_tenant_id"
        timestamp = datetime.now()
        location = Location().dict()
        location["status"] = location["status"].value

        artifact_body = {
            "device_id": device_id,
            "tenant_id": tenant_id,
            "timestamp": timestamp,
            "event_name": "com.bosch.ivs.incident.IncidentEvent",
            "location": location,
            "incident_type": "INCIDENT_TYPE__ACCIDENT_AUTOMATIC"
        }

        collection_events_mock = Mock()
        metadata_collections = Mock()
        metadata_collections.events = collection_events_mock

        # WHEN
        process_sanitizer(artifact_body, metadata_collections)

        # THEN
        collection_events_mock.insert_one.assert_called_once_with(artifact_body)

    @pytest.mark.unit
    def test_create_sav_operator(self):
        # GIVEN
        disconnect("DataIngestionDB")
        with connect(db="DataIngestionDB", host="mongomock://localhost", alias="DataIngestionDB"):

            event_timestamp = datetime.fromisoformat("2023-08-29T08:17:15+00:00",)
            operator_monitoring_start = datetime.fromisoformat("2023-08-29T08:18:49+00:00")
            operator_monitoring_end = datetime.fromisoformat("2023-08-29T08:35:57+00:00")

            artifact_body_sav_people_count = {
                "tenant_id": "datanauts",
                "device_id": "DATANAUTS_DEV_02",
                "event_timestamp": event_timestamp,
                "operator_monitoring_start": operator_monitoring_start,
                "operator_monitoring_end": operator_monitoring_end,
                "artifact_name": "sav-operator-people-count",
                "additional_information": {
                    "is_door_blocked": True,
                    "is_camera_blocked": False,
                    "is_audio_malfunction": True,
                    "observations": "foo"
                },
                "is_people_count_correct": False,
                "correct_count": 5
            }

            artifact_body_sav_sos = {
                "tenant_id": "datanauts",
                "device_id": "DATANAUTS_DEV_02",
                "event_timestamp": event_timestamp,
                "operator_monitoring_start": operator_monitoring_start,
                "operator_monitoring_end": operator_monitoring_end,
                "artifact_name": "sav-operator-sos",
                "additional_information": {
                    "is_door_blocked": True,
                    "is_camera_blocked": False,
                    "is_audio_malfunction": True,
                    "observations": "foo"
                },
                "reason": "OTHER"
            }
            di["db_metadata_tables"] = {"sav_operator_feedback": "dev-sav-operator-feedback"}

            metadata_collections = Mock()
            # WHEN
            process_sanitizer(artifact_body_sav_people_count, metadata_collections)
            process_sanitizer(artifact_body_sav_sos, metadata_collections)

            # THEN
            query = {
                "tenant_id": "datanauts",
                "device_id": "DATANAUTS_DEV_02"
            }
            from metadata.consumer.database.operator_feedback import DBPeopleCountOperatorArtifact
            from metadata.consumer.database.operator_feedback import DBSOSOperatorArtifact
            db_artifacts_people_count = DBPeopleCountOperatorArtifact.objects(**query)
            db_artifacts_sos = DBSOSOperatorArtifact.objects(**query)
            assert len(db_artifacts_people_count) == 1
            assert len(db_artifacts_sos) == 1

    @pytest.mark.parametrize("file_format,filepath,anonymized_path,voxel_dataset_name",
                             [*[(file_format,
                                 f"s3://a/b/c/dmp4.{file_format}",
                                 f"s3://anon_bucket/b/c/dmp4_anonymized.{file_format}",
                                 "Debug_Lync_snapshots") for file_format in IMAGE_FORMATS],
                              *[(file_format,
                                 f"s3://a/b/c/d.{file_format}",
                                 f"s3://anon_bucket/b/c/d_anonymized.{file_format}",
                                 "Debug_Lync") for file_format in VIDEO_FORMATS],
                              *[(file_format,
                                 f"s3://a/ridecare_companion_gridwise/c/d.{file_format}",
                                 f"s3://anon_bucket/ridecare_companion_gridwise/c/d_anonymized.{file_format}",
                                 "RC-ridecare_companion_gridwise_snapshots") for file_format in IMAGE_FORMATS],
                              *[(file_format,
                                 f"s3://a/ridecare_companion_gridwise/c/d.{file_format}",
                                 f"s3://anon_bucket/ridecare_companion_gridwise/c/d_anonymized.{file_format}",
                                 "RC-ridecare_companion_gridwise") for file_format in VIDEO_FORMATS]
                              ])
    @patch("metadata.consumer.voxel.functions.update_sample")
    @patch("metadata.consumer.voxel.functions.create_dataset")
    @patch.dict("metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket", "RAW_S3": "raw_bucket"})
    @patch.dict("metadata.consumer.main.os.environ",
                {"TENANT_MAPPING_CONFIG_PATH": get_abs_path(__file__, "test_data/config.yml")})
    @patch.dict("metadata.consumer.main.os.environ",
                {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
    @pytest.mark.unit
    def test_update_voxel_media(  # pylint: disable=too-many-arguments
            self,
            mock_create_dataset_voxel: Mock,
            mock_update_sample_voxel: Mock,
            file_format: dict,
            filepath: str,
            anonymized_path: str,
            voxel_dataset_name: str):
        # Given
        di.clear_cache()
        bootstrap_di()
        recording_item: dict = {
            "_id": "test",
            "filepath": filepath
        }
        sample = recording_item.copy()
        sample.pop("_id")
        sample["s3_path"] = anonymized_path
        sample["raw_filepath"] = filepath

        # When
        update_voxel_media(recording_item)

        # Then
        mock_create_dataset_voxel.assert_called_once_with(
            voxel_dataset_name, ["RC"])
        mock_update_sample_voxel.assert_called_once_with(
            voxel_dataset_name, sample)

    @patch("metadata.consumer.main.ContainerServices.download_file",
           return_value=json.dumps({"a": "b"}).encode("UTF-8"))
    @pytest.mark.unit
    def test_upsert_mdf_data(self, _: Mock):
        # Given
        mock_collection_rec = Mock()
        mock_collection_rec.find_one_and_update = Mock()
        mock_collection_sig = Mock()
        mock_collection_sig.update_one = Mock()
        message = json.loads(_read_test_fixture(
            "input_raw_message_body_metadata_mdfparser"))

        metadata_collections = MetadataCollections(
            signals=mock_collection_sig,
            recordings=mock_collection_rec,
            pipeline_exec=MagicMock(),
            algo_output=MagicMock(),
            processed_imu=MagicMock(),
            events=MagicMock()
        )

        # When
        recording = upsert_mdf_signals_data(message, metadata_collections)

        # Then
        assert recording is not None
        mock_collection_rec.find_one_and_update.assert_called_once_with(
            filter={
                "video_id": "datanauts_DATANAUTS_DEV_02_InteriorRecorder_1680540223210_1680540250651"},
            update={
                "$set": {
                    "recording_overview.chc_duration": 26.478034,
                    "recording_overview.gnss_coverage": 0.0,
                    "recording_overview.max_audio_loudness": -32.2027,
                    "recording_overview.max_person_count": 1,
                    "recording_overview.mean_audio_bias": 0.36789289230769234,
                    "recording_overview.median_person_count": 0.0,
                    "recording_overview.number_chc_events": 1,
                    "recording_overview.ride_detection_people_count_before": -1,
                    "recording_overview.ride_detection_people_count_after": 0,
                    "recording_overview.sum_door_closed": 0,
                    "recording_overview.variance_person_count": 0.04}},
            upsert=True,
            return_document=True)
        mock_collection_sig.update_one.assert_called_once_with(
            filter={
                "recording": "datanauts_DATANAUTS_DEV_02_InteriorRecorder_1680540223210_1680540250651",
                "source": {
                    "$regex": "MDF.*"}},
            update={
                "$set": {
                    "source": "MDFParser",
                    "signals": {
                        "a": "b"}}},
            upsert=True)

    @pytest.mark.unit
    @patch("metadata.consumer.voxel.functions.update_sample")
    @patch("metadata.consumer.voxel.functions.create_dataset")
    @patch("metadata.consumer.main.download_and_synchronize_chc", return_value=({"a": "b"}, {"c": "d"}))
    @patch.dict("metadata.consumer.main.os.environ",
                {"TENANT_MAPPING_CONFIG_PATH": get_abs_path(__file__, "test_data/config.yml")})
    @patch.dict("metadata.consumer.main.os.environ",
                {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
    @patch.dict("metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket", "RAW_S3": "raw_bucket"})
    def test_process_outputs(
            self,
            mock_download_and_synchronize_chc: Mock,
            mock_create_dataset_voxel: Mock,
            mock_update_sample_voxel: Mock,):
        # Given
        video_id = "id"
        message = {
            "s3_path": "s3://bucket/a/b/c.mp4",
            "output": {
                "bucket": "a",
                "meta_path": "e/f.media"
            }
        }
        bootstrap_di()
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
            "results": {
                "CHBs_sync": {
                    "a": "b"
                },
                "CHC_metrics": {
                    "c": "d"
                }
            }
        }

        expected_set_mongodb = copy.deepcopy(expected_out)

        expected_voxel_output = {
            **expected_out,
            "algorithms": {
                "id_CHC": {
                    "output_paths": expected_out["output_paths"],
                    "results": expected_out["results"]
                }
            },
            "s3_path": "s3://anon_bucket/a/b/c_anonymized.mp4",
            "raw_filepath": "s3://bucket/a/b/c.mp4",
            "video_id": "id"
        }

        expected_voxel_output.pop("results")
        expected_out.pop("output_paths")

        metadata_collections = MetadataCollections(
            recordings=collection_recordings,
            signals=collection_signals,
            algo_output=collection_algo_out,
            pipeline_exec=MagicMock(),
            processed_imu=MagicMock(),
            events=MagicMock()
        )

        # When
        process_outputs(video_id, message, metadata_collections, source)

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
            {"_id": "id_CHC"},
            {
                "$set": expected_set_mongodb
            }, upsert=True)
        mock_download_and_synchronize_chc.assert_called_once_with(
            "id", collection_recordings, "a", "e/f.media")
        mock_create_dataset_voxel.assert_called_once_with("Debug_Lync", ["RC"])
        mock_update_sample_voxel.assert_called_once_with(
            "Debug_Lync", expected_voxel_output)

    @pytest.mark.unit
    @pytest.mark.skip(reason="Not worth updateing the test before refactoring")
    @pytest.mark.parametrize("input_message_body,input_message_atributes,return_upserts", [
        (
            _snapshot_sdr_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore # pylint: disable=line-too-long
            _message_attributes_body(),  # type: ignore
            True
        ),
        (
            _snapshot_sdr_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore # pylint: disable=line-too-long
            _message_attributes_body(),  # type: ignore
            False
        ),
        (
            _mdf_metadata_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore # pylint: disable=line-too-long
            _message_attributes_body("MDFParser"),  # type: ignore
            True
        ),
        (
            _mdf_imu_message_body(
                "deepsensation_ivs_slimscaley_develop_bic2hi_01_InteriorRecorder_1647260354251_1647260389044"),   # type: ignore # pylint: disable=line-too-long
            _message_attributes_body("MDFParser"),  # type: ignore
            False
        ),
    ])
    @patch("metadata.consumer.main.create_recording_item")
    @patch("metadata.consumer.main.upsert_mdf_signals_data")
    @patch("metadata.consumer.main.update_voxel_media")
    @patch("metadata.consumer.main.add_voxel_snapshot_metadata")
    @patch.dict("metadata.consumer.main.os.environ", {"TENANT_MAPPING_CONFIG_PATH": "./config/config.yml"})
    def test_upsert_data_to_db_sdr_mdf(  # pylint: disable=too-many-arguments
            self,
            add_voxel_snapshot_metadata_mock: Mock,
            mock_update_voxel_media: Mock,
            mock_upsert_mdf_data: Mock,
            mock_create_recording_item: Mock,
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
        mock_create_recording_item.return_value = return_upserts
        mock_upsert_mdf_data.return_value = return_upserts
        db_mock.__getitem__ = Mock(
            side_effect=[
                collection_signals_mock,
                collection_recordings_mock,
                "pipeline_exec",
                "algo_output",
                "processed_imu"])
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
            mock_create_recording_item.assert_called_once_with(
                input_message_body,
                collection_recordings_mock,
                related_metadata_service_mock)
            if input_message_body["s3_path"].endswith(".jpeg") and return_upserts:
                add_voxel_snapshot_metadata_mock.assert_called_once_with(
                    input_message_body["_id"], input_message_body["s3_path"], input_message_body["metadata_path"])

        else:
            mock_upsert_mdf_data.assert_called_once_with(
                input_message_body, ANY)

        if return_upserts:
            mock_update_voxel_media.assert_called_once_with(return_upserts)
        else:
            mock_update_voxel_media.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize("input_message_body,input_message_atributes", [
    ])
    @patch("metadata.consumer.main.update_pipeline_db")
    @patch("metadata.consumer.main.process_outputs")
    @patch("metadata.consumer.voxel.functions.add_voxel_snapshot_metadata")
    def test_upsert_data_to_db(
            self,
            add_voxel_snapshot_metadata_mock: Mock,
            mock_process_outputs: Mock,
            update_pipeline_db: Mock,
            input_message_body: dict,
            input_message_atributes: dict,
    ):

        # GIVEN
        db_mock = Mock()
        collection_recordings_mock = Mock()
        collection_algo_output_mock = Mock()
        collection_pipeline_exec_mock = Mock()
        collection_signals_mock = Mock()
        container_services_mock = Mock()
        container_services_mock.db_tables = MagicMock()
        recording_id = os.path.basename(
            input_message_body["s3_path"]).split(".")[0]
        db_mock.__getitem__ = Mock(
            side_effect=[
                collection_signals_mock,
                collection_recordings_mock,
                collection_pipeline_exec_mock,
                collection_algo_output_mock])
        related_metadata_service_mock = Mock()

        # THEN
        upsert_data_to_db(db_mock, container_services_mock,
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
            mock_process_outputs.assert_called_once_with(
                recording_id,
                input_message_body,
                collection_algo_output_mock,
                collection_recordings_mock,
                collection_signals_mock,
                source)
        else:
            mock_process_outputs.assert_not_called()

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
        parsed_message = parse_message_body_to_dict(raw_message)
        got_relay_data = fix_message(container_services_mock, raw_message, parsed_message)

        assert got_relay_data == expected_message
        container_services_mock.display_processed_msg.assert_called_once()

    @pytest.fixture(name="mock_boto3_client")
    def fixture_boto3_client(self, mocker: MockerFixture) -> Mock:
        """Mock boto3 AWS client."""
        return mocker.patch("boto3.client")

    @pytest.fixture(name="mock_persistence")
    def fixture_persistence(self, mocker: MockerFixture) -> Mock:
        """Mock Persistence class."""
        return mocker.patch("metadata.consumer.main.Persistence")

    @pytest.fixture(name="mock_related_media_service")
    def fixture_related_media_service(self, mocker: MockerFixture) -> Mock:
        """Mock RelatedMediaservice class."""
        return mocker.patch("metadata.consumer.main.RelatedMediaService")

    @pytest.fixture(name="mock_base")
    def fixture_base(self, mocker: MockerFixture) -> Mock:
        """Mock base package."""
        return mocker.patch("base")

    @pytest.fixture(name="mock_graceful_exit")
    def fixture_graceful_exit(self, mocker: MockerFixture) -> Mock:
        """Mock GracefulExit class."""
        return mocker.patch("metadata.consumer.main.GracefulExit")

    @pytest.fixture(name="mock_parse_message")
    def fixture_parse_message(self, mocker: MockerFixture) -> Mock:
        """Mock parse_message function."""
        return mocker.patch("metadata.consumer.main.parse_message_body_to_dict")

    @pytest.fixture(name="mock_fix_message")
    def fixture_fix_message(self, mocker: MockerFixture) -> Mock:
        """Mock fix_message function."""
        return mocker.patch("metadata.consumer.main.fix_message")

    @pytest.fixture(name="mock_upsert_data_to_db")
    def fixture_upsert_data_to_db(self, mocker: MockerFixture) -> Mock:
        """Mock upsert_data_to_db function."""
        return mocker.patch("metadata.consumer.main.upsert_data_to_db")

    @pytest.fixture(name="mock_container_services")
    def fixture_container_services(self, mocker: MockerFixture) -> Mock:
        """Mock ContainerServices class."""
        return mocker.patch("metadata.consumer.main.ContainerServices")

    @pytest.fixture(name="mock_continue_running")
    def fixture_continue_running(self, mocker: MockerFixture) -> Mock:
        """Mock GracefulExit.continue_running property."""
        return mocker.patch("metadata.consumer.main.GracefulExit.continue_running",
                            new_callable=PropertyMock, side_effect=[True, False])

    @pytest.mark.unit
    @patch("metadata.consumer.main.connect")
    @patch.dict("metadata.consumer.main.os.environ",
                {"TENANT_MAPPING_CONFIG_PATH": get_abs_path(__file__, "test_data/config.yml")})
    @patch.dict("metadata.consumer.main.os.environ", {"FIFTYONE_DATABASE_URI": "DB_URI"})
    @patch.dict("metadata.consumer.main.os.environ",
                {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
    def test_metadata_consumer_main(  # pylint: disable=too-many-arguments,unused-argument
            self,
            mock_connect: Mock,
            mock_container_services: Mock,
            mock_boto3_client: Mock,
            mock_persistence: Mock,
            mock_related_media_service: Mock,
            mock_parse_message: Mock,
            mock_fix_message: Mock,
            mock_upsert_data_to_db: Mock,
            mock_continue_running: Mock):
        """Test metadata.consumer.main

        Args:
            mock_container_services (Mock): mocked container services object
            mock_boto3_client (Mock): mocked boto3 client
            mock_persistence (Mock): mocked persistence object
            mock_related_media_service (Mock): mocked media service object
            mock_fix_message (Mock): mocked read_message function
            mock_upsert_data_to_db (Mock): mocked upser_data_to_db function
            mock_continue_running (Mock): mocked continue_running property from GracefulExit
        """
        input_message = _metadata_sqs_message_helper({
            "dummy_body": "dummy_body_value",
        })
        sqs_client_mock = Mock()
        s3_client_mock = Mock()
        mock_boto3_client.side_effect = [s3_client_mock, sqs_client_mock]
        mock_container_services_object = MagicMock()
        mock_container_services.return_value = mock_container_services_object
        mock_container_services_object.load_config_vars = Mock()
        mock_container_services_object.load_mongodb_config_vars = Mock()
        mock_db_client = MagicMock()
        mock_db_client.__getitem__.side_effect = [
            Mock(), Mock(), Mock(), Mock(), Mock(), Mock(), Mock()
        ]
        mock_db_client.client = Mock()
        mock_container_services_object.create_db_client = Mock(
            return_value=mock_db_client)
        mock_container_services_object.db_tables.__getitem__.side_effect = [
            Mock(), Mock(), Mock(), Mock(), Mock(), Mock(), Mock()
        ]
        mock_container_services_object.anonymized_s3 = "anon_bucket"
        mock_container_services_object.raw_s3 = "raw_bucket"
        mock_container_services_object.delete_message = Mock()
        mock_container_services_object.get_single_message_from_input_queue = Mock(
            return_value=input_message)
        mock_persistence_object = Mock()
        mock_persistence.return_value = mock_persistence_object
        mock_api_service = Mock()
        mock_related_media_service.return_value = mock_api_service
        mock_relay_list = Mock()
        mocked_parsed_message = Mock()
        mocked_parsed_message.copy = Mock()
        mock_parse_message.return_value = mocked_parsed_message
        mock_fix_message.return_value = mock_relay_list

        main.main()

        mock_boto3_client.assert_any_call("sqs", region_name=AWS_REGION, endpoint_url=None)
        mock_container_services_object.load_config_vars.assert_called_once_with()
        mock_container_services_object.load_mongodb_config_vars.assert_called_once_with()
        mock_container_services_object.create_db_client.assert_called_once_with()
        mock_persistence.assert_called_once_with(
            mock_container_services_object.db_tables, mock_db_client.client)
        mock_related_media_service.assert_called_once_with(
            mock_persistence_object)
        mock_container_services_object.get_single_message_from_input_queue.assert_called_once_with(
            sqs_client_mock)
        mock_parse_message.assert_called_once_with(input_message["Body"])
        mock_fix_message.assert_called_once_with(
            mock_container_services_object, input_message["Body"], mocked_parsed_message.copy.return_value)
        mock_upsert_data_to_db.assert_called_once_with(
            mock_api_service,
            mock_relay_list,
            input_message["MessageAttributes"],
            ANY)
        mock_connect.assert_called_once_with(db="DataIngestion", host="DB_URI", alias="DataIngestionDB")
        mock_container_services_object.delete_message.assert_called_once_with(
            sqs_client_mock, input_message["ReceiptHandle"])


@pytest.mark.unit
@patch.dict("metadata.consumer.main.os.environ", {"ANON_S3": "anon_bucket", "RAW_S3": "raw_bucket"})
@patch.dict("metadata.consumer.main.os.environ",
            {"MONGODB_CONFIG": get_abs_path(__file__, "test_data/mongo_config.yml")})
@patch("metadata.consumer.voxel.functions.update_sample")
@patch("metadata.consumer.voxel.functions.create_dataset")
@patch("metadata.consumer.main.download_and_synchronize_chc")
def test_process_outputs_chc_document_too_large(download_and_sync: Mock,
                                                create_dataset_mock: Mock,
                                                update_sample_mock: Mock):
    video_id = "video_id"
    algo_out_collection = MagicMock()
    pipeline_exec_collection = MagicMock()
    signals_collection = MagicMock()
    metadata_collections = MetadataCollections(
        recordings=MagicMock(),
        pipeline_exec=pipeline_exec_collection,
        signals=signals_collection,
        algo_output=algo_out_collection,
        processed_imu=MagicMock(),
        events=MagicMock()
    )
    mock_message = {
        "output": {
            "bucket": "wow",
            "meta_path": "wow/yeah"
        },
        "s3_path": "s3://anon_bucket/wow/yeah.mp4"
    }
    download_and_sync.return_value = ({}, {})

    signals_collection.update_one = Mock(side_effect=DocumentTooLarge)
    pipeline_exec_collection.find_one_and_update = Mock()

    process_outputs(video_id, mock_message, metadata_collections, "CHC")

    pipeline_exec_collection.find_one_and_update.assert_called_once_with(
        filter={"video_id": video_id},
        update={"$set": {"data_status": "error"}}
    )
    create_dataset_mock.assert_called_once()
    update_sample_mock.assert_called_once()


@pytest.mark.parametrize("file_exists", [
    (True),
    (False)
])
@pytest.mark.unit
def test_insert_mdf_imu_data(file_exists: bool):
    """ tests insert_mdf_imu_data """
    s3_client = Mock()
    stream_body_mock = Mock()
    stream_body_mock.read = Mock(return_value=bytes("""[
        {"timestamp": 1692000444123, "source": {"tenant":"foo","device_id":"bar"}},
        {"timestamp": 1692000444523, "source": {"tenant":"foo","device_id":"bar"}}
        ]""", "utf-8"))
    s3_client.get_object = Mock(return_value={"Body": stream_body_mock})
    s3_client.delete_object = Mock()

    if not file_exists:
        s3_client.head_object = Mock(side_effect=ClientError(
            {"Error": {"Code": "404"}}, operation_name="Mock_error"))
    else:
        s3_client.head_object = Mock(return_value={})

    imu_message: dict = {
        "_id": "foo",
        "parsed_file_path": "s3://bucket/dir/parsed_imu.json",
        "data_type": "imu",
        "recording_overview": {
            "foo": "bar"
        }
    }

    mock_imu_col = Mock()
    mock_imu_col.insert_many = Mock()
    mock_events_col = Mock()
    mock_events_col.update_many = Mock()

    metadata_collections = MetadataCollections(
        signals=MagicMock(),
        recordings=MagicMock(),
        algo_output=MagicMock(),
        pipeline_exec=MagicMock(),
        processed_imu=mock_imu_col,
        events=mock_events_col
    )

    di["s3_client"] = s3_client
    main.__process_mdfparser(imu_message, metadata_collections)
    di.clear_cache()

    s3_client.head_object.assert_called_once_with(
        Bucket="bucket", Key="dir/parsed_imu.json")

    if file_exists:
        s3_client.get_object.assert_called_once_with(
            Bucket="bucket", Key="dir/parsed_imu.json")
        mock_imu_col.insert_many.assert_called_once()
        from_ts = datetime.fromtimestamp(1692000444.123, tz=UTC)
        to_ts = datetime.fromtimestamp(1692000444.523, tz=UTC)
        mock_events_col.update_many.assert_has_calls([
            call(filter={"$and": [
                {"last_shutdown.timestamp": {"$exists": False}},
                {"tenant_id": "foo"},
                {"device_id": "bar"},
                {"timestamp": {"$gte": from_ts}},
                {"timestamp": {"$lte": to_ts}},
            ]},
                update={
                    "$set": {"imu_available": True}
            }),
            call(
                filter={"$and": [
                    {"last_shutdown.timestamp": {"$exists": True}},
                    {"last_shutdown.timestamp": {"$ne": None}},
                    {"tenant_id": "foo"},
                    {"device_id": "bar"},
                    {"last_shutdown.timestamp": {"$gte": from_ts}},
                    {"last_shutdown.timestamp": {"$lte": to_ts}},
                ]},
                update={
                    "$set": {"last_shutdown.imu_available": True}
                })
        ]
        )
    else:
        s3_client.get_object.assert_not_called()
        mock_imu_col.insert_many.assert_not_called()
        mock_events_col.update_many.assert_not_called()
