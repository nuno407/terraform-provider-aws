"""Test db module."""
# pylint: disable=missing-function-docstring,missing-module-docstring
from typing import Tuple

import mongomock
import pytest
from pymongo.collection import Collection
from base.aws.container_services import DATA_INGESTION_DATABASE_NAME
from metadata.api.db import Persistence
from tests.common import db_tables

OUR_RECORDING = "our_recording"
OTHER_RECORDING = "other_recording"
OUR_ALGO = "what_we_are_looking_for"
OTHER_ALGO = "what_we_were_not_looking_for"


@pytest.fixture(scope="function", name="mock_persistence")
def persistence() -> Tuple[Persistence, mongomock.MongoClient]:
    client = mongomock.MongoClient()
    return Persistence(None, db_tables, client), client


@pytest.mark.unit
def test_update_recording_description(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings

    recordings.insert_one(
        {"video_id": "foo", "recording_overview": {"description": "bar"}})

    database.update_recording_description("foo", "Hello World!")

    recording = recordings.find_one({"video_id": "foo"})
    assert recording["recording_overview"]["description"] == "Hello World!"


@pytest.mark.unit
def test_get_algo_output(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    algo_output: Collection = client[DATA_INGESTION_DATABASE_NAME].algo_output

    algo_output.insert_one(
        {"algorithm_id": OTHER_ALGO, "pipeline_id": OUR_RECORDING, "content": "def"})
    algo_output.insert_one(
        {"algorithm_id": OUR_ALGO, "pipeline_id": OTHER_RECORDING, "content": "def"})
    correct_id = algo_output.insert_one(
        {"algorithm_id": OUR_ALGO, "pipeline_id": OUR_RECORDING, "content": "abc"}).inserted_id
    algo_output.insert_one(
        {"algorithm_id": OTHER_ALGO, "pipeline_id": OTHER_RECORDING, "content": "abc"})

    entry = database.get_algo_output(
        algo=OUR_ALGO, recording_id=OUR_RECORDING)

    assert entry["_id"] == correct_id
    assert entry["content"] == "abc"


def prepare_recordings_data(recordings, pipeline_execs, status="complete"):
    pipeline_execs.insert_one({
        "_id": OTHER_RECORDING,
        "from_container": OTHER_ALGO,
        "processing_list": ["CHC", "Anonymize"],
        "data_status": status
    })
    pipeline_execs.insert_one({
        "_id": OUR_RECORDING,
        "from_container": OUR_ALGO,
        "processing_list": ["Anonymize"],
        "data_status": status
    })
    recordings.insert_one({
        "_id": OUR_RECORDING,
        "video_id": OUR_RECORDING,
        "_media_type": "video",
        "recording_overview": {"time": "0:05:00"}
    })
    recordings.insert_one({
        "_id": OTHER_RECORDING,
        "video_id": OTHER_RECORDING,
        "_media_type": "video",
        "recording_overview": {"time": "0:08:00"}
    })


@pytest.mark.unit
def test_get_single_recording(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings
    pipeline_execs: Collection = client[DATA_INGESTION_DATABASE_NAME].pipeline_exec
    prepare_recordings_data(recordings, pipeline_execs)

    recording = database.get_single_recording(OUR_RECORDING)

    assert recording["_id"] == OUR_RECORDING
    processing_list = recording["pipeline_execution"]["processing_list"]
    assert len(processing_list) == 1
    assert "Anonymize" in processing_list
    assert recording["recording_overview"]["time"] == "0:05:00"


@pytest.mark.unit
def test_get_single_recording_fails_if_not_found(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordingss
    pipeline_execs: Collection = client[DATA_INGESTION_DATABASE_NAME].pipeline_execs

    prepare_recordings_data(recordings, pipeline_execs, "failed")

    with pytest.raises(LookupError):
        database.get_single_recording(OUR_RECORDING)


@pytest.mark.unit
def test_get_recording_list(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings
    pipeline_execs: Collection = client[DATA_INGESTION_DATABASE_NAME].pipeline_exec

    prepare_recordings_data(recordings, pipeline_execs)

    recording_list, total, pages = database.get_recording_list(
        10, 1, None, None)

    assert len(recording_list) == 2
    assert total == 2
    assert pages == 1
    assert (OUR_RECORDING in rec["_id"] for rec in recording_list)
    assert (OTHER_RECORDING in rec["_id"] for rec in recording_list)


@pytest.mark.unit
def test_get_recording_list_paged(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings
    pipeline_execs: Collection = client[DATA_INGESTION_DATABASE_NAME].pipeline_exec
    prepare_recordings_data(recordings, pipeline_execs)

    recording_list, total, pages = database.get_recording_list(
        1, 1, None, None)

    assert len(recording_list) == 1
    assert total == 2
    assert pages == 2
    assert (OUR_RECORDING in rec["_id"] for rec in recording_list)
    assert (OTHER_RECORDING in rec["_id"] for rec in recording_list)


@pytest.mark.unit
@pytest.mark.skip
# FIXME: should recordings with pipeline exec marked as failed not be
# returned by get_recording_list? currently they"re not being filtered.
def test_get_empty_recording_list(mock_persistence):
    database: Persistence = mock_persistence[0]
    client: mongomock.MongoClient = mock_persistence[1]
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings
    pipeline_execs: Collection = client[DATA_INGESTION_DATABASE_NAME].pipeline_exec
    prepare_recordings_data(recordings, pipeline_execs, "failed")

    recording_list, total, pages = database.get_recording_list(
        10, 1, None, None)

    assert len(recording_list) == 0
    assert total == 0
    assert pages == 0
