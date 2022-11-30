import os
from tests.common import db_tables
import pytest
import mongomock
from bson.json_util import loads
from pymongo.collection import Collection
from metadata.consumer.db import Persistence
from base.aws.container_services import DATA_INGESTION_DATABASE_NAME

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


@pytest.fixture()
def recordings_persistence() -> tuple[Persistence, mongomock.MongoClient]:
    client = mongomock.MongoClient()
    recordings: Collection = client[DATA_INGESTION_DATABASE_NAME].recordings

    with open(os.path.join(__location__, "test_data/recording_image_db.json"), "r") as f:
        data = loads(f.read())
        recordings.insert(data)

    with open(os.path.join(__location__, "test_data/recording_video_db.json"), "r") as f:
        data = loads(f.read())
        recordings.insert(data)

    return Persistence(None, db_tables, client)


@ pytest.mark.unit
def test_related_media_service_image(recordings_persistence):
    # GIVEN
    device = "rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed"
    tenant = "ridecare_companion_trial"

    start_ms = 1664480143 * 1000
    media_type = "image"

    videos_paths = [
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_InteriorRecorder_1664480133262_1664480395622",
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingRecorder_1664480013262_1664480515622"]

    # WHEN
    result = recordings_persistence.get_video_snapshot_media(device, tenant, start_ms, None, media_type)

    # THEN
    assert result == videos_paths


@ pytest.mark.unit
def test_related_media_service_video(recordings_persistence):
    # GIVEN
    device = "rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed"
    tenant = "ridecare_companion_trial"

    start_ms = 1664480130 * 1000
    end_ms = 1664480399 * 1000
    media_type = "video"

    snapshots_paths = [
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingMultiSnapshot_TrainingMultiSnapshot-8160e619-be7e-4d0f-987d-a2a5292e7a24_28_1664480356000",
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingMultiSnapshot_TrainingMultiSnapshot-8160e619-be7e-4d0f-987d-a2a5292e7a24_24_1664480150000",
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingMultiSnapshot_TrainingMultiSnapshot-8160e619-be7e-4d0f-987d-a2a5292e7a24_25_1664480320000",
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingMultiSnapshot_TrainingMultiSnapshot-8160e619-be7e-4d0f-987d-a2a5292e7a24_26_1664480325000",
        "ridecare_companion_trial_rc_srx_prod_8f8b793d1b290e4045d0c478f74960acd91cceed_TrainingMultiSnapshot_TrainingMultiSnapshot-8160e619-be7e-4d0f-987d-a2a5292e7a24_27_1664480327000"]

    # WHEN
    result = recordings_persistence.get_video_snapshot_media(device, tenant, start_ms, end_ms, media_type)

    # THEN
    assert result == snapshots_paths
