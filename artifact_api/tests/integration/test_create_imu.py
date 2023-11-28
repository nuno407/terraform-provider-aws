# pylint: disable=missing-function-docstring,missing-module-docstring,missing-class-docstring,line-too-long
from datetime import datetime, timedelta
from pytz import UTC

import pytest
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient
from mongomock import MongoClient as MongoMockClient

from base.model.artifacts import IncidentEventArtifact, IMUProcessingResult
from base.model.metadata.api_messages import IMUDataArtifact
from imu_test_builder import ImuTestDataBuilder

IMU_TIMESTAMP_FROM = datetime.now(tz=UTC) - timedelta(minutes=15)
IMU_TIMESTAMP_TO = datetime.now(tz=UTC) - timedelta(minutes=5)
EVENT_TIMESTAMP_INCLUDED = datetime.now(tz=UTC) - timedelta(minutes=10)
EVENT_TIMESTAMP_EXCLUDED = datetime.now(tz=UTC) - timedelta(hours=2)
TENANT = "tenant"


@pytest.mark.integration
class TestCreateImu:
    def input_event(self, timestamp: datetime, device_id: str) -> IncidentEventArtifact:
        return IncidentEventArtifact(tenant_id=TENANT,
                                     device_id=device_id,
                                     timestamp=timestamp,
                                     location=None,
                                     incident_type="INCIDENT_TYPE__PANIC",
                                     bundle_id="foo")

    @pytest.fixture
    def input_imu(self) -> IMUDataArtifact:
        artifact = IMUProcessingResult(
            s3_path="s3://bucket-raw-video-files/tenant/device_TrainingRecorder_ridecare_device_recording_1662080172308_1662080561893.mp4",
            correlation_id="some_video_id")
        data = ImuTestDataBuilder().with_frames(100).build()
        return IMUDataArtifact(message=artifact, data=data)

    @pytest.mark.integration
    @pytest.mark.skip(reason="To be tackled in imu US")
    def test_create_imu(
            self,
            api_client: TestClient,
            input_imu: IMUDataArtifact,
            mongo_client: MongoMockClient):
        # WHEN
        insert_imu_result = api_client.post(
            "/ridecare/imu/video", json=jsonable_encoder(input_imu))

        # THEN
        assert insert_imu_result.status_code == 201

        # assert that the right count of IMU samples has been ingested
        imu_entries = list(mongo_client["DataIngestion"]["processed-imu"].find(
            {"timestamp": {"$gte": IMU_TIMESTAMP_FROM, "$lte": IMU_TIMESTAMP_TO}}))
        assert len(imu_entries) == 100

        # assert that the tenant and device information is present in the first entry
        assert imu_entries[0]["source"]["tenant"] == TENANT
        assert imu_entries[0]["source"]["device_id"] == "device"

    @pytest.mark.integration
    @pytest.mark.skip(reason="To be tackled in imu US")
    def test_events_are_updated_on_imu_ingestion(self,
                                                 api_client: TestClient,
                                                 input_imu: IMUDataArtifact,
                                                 mongo_client: MongoMockClient):
        # GIVEN
        mongo = mongo_client

        event_inside_imu_boundaries = self.input_event(EVENT_TIMESTAMP_INCLUDED, "device")
        event_outside_imu_boundaries = self.input_event(EVENT_TIMESTAMP_EXCLUDED, "device")
        event_from_other_device = self.input_event(EVENT_TIMESTAMP_INCLUDED, "other_device")

        # WHEN
        # insert events
        for event in [event_inside_imu_boundaries, event_outside_imu_boundaries, event_from_other_device]:
            event_result = api_client.post("/ridecare/event", json=jsonable_encoder(event))
            assert event_result.status_code == 201
        # insert IMU
        insert_imu_result = api_client.post(
            "/ridecare/imu/video", json=jsonable_encoder(input_imu))

        # THEN
        assert insert_imu_result.status_code == 201

        # assert that only the event inside the IMU boundaries and belonging to the same device has been updated
        updated_event = mongo["DataIngestion"]["incident-events"].find_one(
            {"timestamp": EVENT_TIMESTAMP_INCLUDED, "device_id": "device"})
        assert updated_event is not None
        assert updated_event.get("IMU_available") is True

        # assert that the other events have not been updated
        event_outside_imu_boundaries = mongo["DataIngestion"]["incident-events"].find_one(
            {"timestamp": EVENT_TIMESTAMP_EXCLUDED})
        assert event_outside_imu_boundaries is not None
        assert event_outside_imu_boundaries.get("IMU_available") is not True

        event_from_other_device = mongo["DataIngestion"]["incident-events"].find_one(
            {"device_id": "other_device"})
        assert event_from_other_device is not None
        assert event_from_other_device.get("IMU_available") is not True
