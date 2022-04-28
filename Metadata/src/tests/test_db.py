import pytest
import mongomock
from api.db import Persistence

db_tables = { 'recording': 'recording', 'pipeline_exec': 'pipeline_exec', 'algo_output': 'algo_output'}
our_recording = 'our_recording'
other_recording = 'other_recording'
our_algo = 'what_we_are_looking_for'
other_algo = 'what_we_were_not_looking_for'

def db_client():
    client = mongomock.MongoClient()
    recordings = client.DB_data_ingestion.recording
    pipeline_execs = client.DB_data_ingestion.pipeline_exec
    algo_outputs = client.DB_data_ingestion.algo_output
    return client, recordings, pipeline_execs, algo_outputs

@pytest.fixture(scope='function')
def persistence():
    client, recordings, pipeline_execs, algo_outputs = db_client()
    return Persistence(None, db_tables, False, client), recordings, pipeline_execs, algo_outputs

def test_get_recording(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    recordings.insert_one({'_id': 'bar', 'content': 'def'})
    recordings.insert_one({'_id': 'foo', 'content': 'abc'})

    # WHEN
    recording = db.get_recording('foo')

    # THEN
    assert(recording['_id'] == 'foo')
    assert(recording['content'] == 'abc')

def test_get_algo_output(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    algo_outputs.insert_one({'algorithm_id': other_algo, 'pipeline_id': our_recording, 'content': 'def'})
    algo_outputs.insert_one({'algorithm_id': our_algo, 'pipeline_id': other_recording, 'content': 'def'})
    correct_id = algo_outputs.insert_one({'algorithm_id': our_algo, 'pipeline_id': our_recording, 'content': 'abc'}).inserted_id
    algo_outputs.insert_one({'algorithm_id': other_algo, 'pipeline_id': other_recording, 'content': 'abc'})

    # WHEN
    recording = db.get_algo_output('what_we_are_looking_for', our_recording)

    # THEN
    assert(recording['_id'] == correct_id)
    assert(recording['content'] == 'abc')

def prepare_recordings_data(recordings, pipeline_execs, status = 'complete'):
    pipeline_execs.insert_one({'from_container': other_algo, 'processing_list':['CHC', 'Anonymize'], '_id': other_recording, 'data_status': status})
    pipeline_execs.insert_one({'from_container': our_algo, 'processing_list':['Anonymize'], '_id': our_recording, 'data_status': status})
    recordings.insert_one({'_id': our_recording, 'recording_overview':{'time': '0:05:00'}})
    recordings.insert_one({'_id': other_recording, 'recording_overview':{'time': '0:08:00'}})

def test_get_single_recording(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    prepare_recordings_data(recordings, pipeline_execs)

    # WHEN
    recording = db.get_single_recording(our_recording)

    # THEN
    assert(recording['_id'] == our_recording)
    processing_list = recording['pipeline_execution']['processing_list']
    assert(len(processing_list) == 1)
    assert('Anonymize' in processing_list)
    assert(recording['recording_overview']['time'] == '0:05:00')

def test_get_single_recording_fails_if_not_found(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    prepare_recordings_data(recordings, pipeline_execs, 'failed')

    # WHEN-THEN
    with pytest.raises(LookupError):
        db.get_single_recording(our_recording)

def test_get_recording_list(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    prepare_recordings_data(recordings, pipeline_execs)

    # WHEN
    recording_list, total, pages = db.get_recording_list(10, 1, None, None)

    # THEN
    assert(len(recording_list) == 2)
    assert(total == 2)
    assert(pages == 1)
    assert(our_recording in rec['_id'] for rec in recording_list)
    assert(other_recording in rec['_id'] for rec in recording_list)

def test_get_recording_list_paged(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    prepare_recordings_data(recordings, pipeline_execs)

    # WHEN
    recording_list, total, pages = db.get_recording_list(1, 1, None, None)

    # THEN
    assert(len(recording_list) == 1)
    assert(total == 2)
    assert(pages == 2)
    assert(our_recording in rec['_id'] for rec in recording_list)
    assert(other_recording in rec['_id'] for rec in recording_list)

def test_get_empty_recording_list(persistence):
    # GIVEN
    db, recordings, pipeline_execs, algo_outputs = persistence
    prepare_recordings_data(recordings, pipeline_execs, 'failed')

    # WHEN
    recording_list, total, pages = db.get_recording_list(10, 1, None, None)

    # THEN
    assert(len(recording_list) == 0)
    assert(total == 0)
    assert(pages == 0)