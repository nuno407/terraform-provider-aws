import pytest
import pytz
from unittest.mock import Mock, ANY
from typing import Optional
from mongomock import MongoClient, Collection
from base.model.artifacts import EventType, Shutdown
from base.model.event_types import ShutdownReason
from base.model.artifacts import DeviceInfoEventArtifact
from sanitizer.models import DeviceInformation
from sanitizer.device_info_db_client import DeviceInfoDBClient
from datetime import datetime, timedelta


def dummy_datetime() -> datetime:
    return datetime(2021, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)


def dummy_device_info_event(ivs_car: Optional[str]) -> DeviceInfoEventArtifact:
    software_versions = [
        {"software_name": "carapplication", "version": "0.0.1"},
        {"software_name": "damageservice", "version": "3.0.0"},
        {"software_name": "ivs_algo_smoke", "version": "3.5"},
        {"software_name": "smokedetectionservice", "version": "1.0.0"}
    ]

    if ivs_car is not None:
        software_versions.append({"software_name": "ivs_car", "version": ivs_car})
    return DeviceInfoEventArtifact(tenant_id="test_tenant_id",
                                   device_id="test_device_id",
                                   timestamp=dummy_datetime(),
                                   event_name=EventType.DEVICE_INFO,
                                   system_report="some git infos",
                                   software_versions=software_versions,
                                   device_type="slimscaley",
                                   last_shutdown=Shutdown(timestamp=None,
                                                          shutdown_reason=ShutdownReason.UNKNOWN,
                                                          shutdown_reason_description=None))


def dummy_device_info_db_entry(ivs_car_version: str) -> DeviceInformation:
    return DeviceInformation(device_id="test_device_id",
                             tenant_id="test_tenant_id",
                             timestamp=dummy_datetime(),
                             ivscar_version=ivs_car_version)


class TestDeviceInfoDBClient:

    @pytest.fixture
    def database_name(self) -> str:
        return "test_database"

    @pytest.fixture
    def collection_name(self) -> str:
        return "test_collection"

    @pytest.fixture
    def mongo_client(self) -> MongoClient:
        return MongoClient(tz_aware=True)

    @pytest.fixture
    def collection_client(self, database_name: str, collection_name: str, mongo_client: MongoClient) -> Collection:
        return mongo_client[database_name][collection_name]

    @pytest.fixture
    def device_info_db(self, collection_client: Collection) -> DeviceInfoDBClient:
        return DeviceInfoDBClient(collection_client)

    @pytest.mark.unit
    def test_get_latest_device_information(self, device_info_db: DeviceInfoDBClient, collection_client: Collection):

        # GIVEN
        early_doc_device_1 = {
            "device_id": "test_device_id1",
            "tenant_id": "test_tenant_id",
            "timestamp": dummy_datetime(),
            "ivscar_version": "1.0"
        }

        later_doc_device_1 = {
            "device_id": "test_device_id1",
            "tenant_id": "test_tenant_id",
            "timestamp": dummy_datetime() + timedelta(hours=1),
            "ivscar_version": "1.1"
        }

        doc_device_2 = {
            "device_id": "test_device_id2",
            "tenant_id": "test_tenant_id",
            "timestamp": dummy_datetime() + timedelta(hours=2),
            "ivscar_version": "1.2"
        }

        collection_client.insert_many([early_doc_device_1, later_doc_device_1, doc_device_2])

        # WHEN
        result_early_device_1 = device_info_db.get_latest_device_information(
            "test_device_id1", "test_tenant_id", dummy_datetime() + timedelta(minutes=10))
        result_later_device_1 = device_info_db.get_latest_device_information(
            "test_device_id1", "test_tenant_id", dummy_datetime() + timedelta(hours=10))
        result_none = device_info_db.get_latest_device_information(
            "test_device_id3", "test_tenant_id", dummy_datetime() + timedelta(hours=10))

        # THEN
        assert result_early_device_1 == DeviceInformation.model_validate(early_doc_device_1)
        assert result_later_device_1 == DeviceInformation.model_validate(later_doc_device_1)
        assert result_none is None

    @pytest.mark.unit
    def test_store_device_information_no_version(
            self,
            device_info_db: DeviceInfoDBClient,
            collection_client: Collection):

        # GIVEN
        device_info_db.get_latest_device_information = Mock()
        collection_client.insert_one = Mock()
        device_info_event_none = dummy_device_info_event(None)

        # WHEN
        result_device_none = device_info_db.store_device_information(device_info_event_none)

        # THEN
        assert result_device_none is False
        device_info_db.get_latest_device_information.assert_not_called()
        collection_client.insert_one.assert_not_called()

    @pytest.mark.unit
    def test_store_device_information_version_already_exists(
            self, device_info_db: DeviceInfoDBClient, collection_client: Collection):

        # GIVEN
        collection_client.insert_one = Mock()
        device_info_event = dummy_device_info_event("1.0")
        device_info_db.get_latest_device_information = Mock(return_value=dummy_device_info_db_entry("1.0"))

        # WHEN
        result = device_info_db.store_device_information(device_info_event)

        # THEN
        device_info_db.get_latest_device_information.assert_called_once_with(
            device_info_event.device_id, device_info_event.tenant_id, ANY)
        collection_client.insert_one.assert_not_called()
        assert result is False

    @pytest.mark.unit
    def test_store_device_information(self, device_info_db: DeviceInfoDBClient, collection_client: Collection):

        # GIVEN
        collection_client.insert_one = Mock()
        device_info_event = dummy_device_info_event("1.0")
        device_info_db_existent_entry = dummy_device_info_db_entry("0.8")
        device_info_db_entry = dummy_device_info_db_entry("1.0")
        device_info_db.get_latest_device_information = Mock(side_effect=[device_info_db_existent_entry, None])

        # WHEN
        result_1 = device_info_db.store_device_information(device_info_event)
        result_2 = device_info_db.store_device_information(device_info_event)

        # THEN
        device_info_db.get_latest_device_information.assert_called_with(
            device_info_event.device_id, device_info_event.tenant_id, ANY)
        assert device_info_db.get_latest_device_information.call_count == 2
        collection_client.insert_one.assert_called_with(device_info_db_entry.model_dump())
        assert collection_client.insert_one.call_count == 2
        assert result_1 is True
        assert result_2 is True
