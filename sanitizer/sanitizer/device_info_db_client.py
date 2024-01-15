"""Manages the device information database"""
from datetime import datetime
from typing import Optional
import logging

import pymongo
from pymongo.collection import Collection
from kink import inject

from base.model.artifacts import DeviceInfoEventArtifact
from sanitizer.models import DeviceInformation

_logger = logging.getLogger(__name__)


@inject
class DeviceInfoDBClient:
    """Manages the device information database"""

    def __init__(self, device_info_collection: Collection):
        """Constructor"""
        self.__collection_client = device_info_collection

    def get_latest_device_information(self, device_id: str, tenant_id: str,
                                      timestamp: datetime) -> Optional[DeviceInformation]:
        """
        Load the latest device information until the timestamp specified.

        Args:
            device_id (str): The device ID
            tenant_id (str): The Tenant ID
            timestamp (datetime): The timestamp upperbound for the search
        """

        latest_document = self.__collection_client.find_one(filter={
            "device_id": device_id,
            "tenant_id": tenant_id,
            "timestamp": {"$lt": timestamp}
        }, sort=[("timestamp", pymongo.DESCENDING)])

        if latest_document is None:
            return None

        return DeviceInformation.model_validate(latest_document)

    def store_device_information(self, device_event: DeviceInfoEventArtifact) -> bool:
        """
        Saves the device information into a diferent collection to be used by the filter.

        Args:
            device_event (DeviceInfoEventArtifact): A DeviceInfoEvent
        """
        software_versions_map = dict(
            map(lambda x: (x["software_name"], x["version"]), device_event.software_versions))

        ivscar_version = software_versions_map.get("ivs_car", None)

        if ivscar_version is None:
            _logger.debug("No ivs_car version found for device=%s", device_event.device_id)
            return False

        latest_info = self.get_latest_device_information(device_event.device_id, device_event.tenant_id, datetime.now())

        if latest_info is not None and latest_info.ivscar_version == ivscar_version:
            _logger.debug("The device version annoucement is present in the database")
            return False

        device_info = DeviceInformation(ivscar_version=ivscar_version,
                                        device_id=device_event.device_id,
                                        tenant_id=device_event.tenant_id,
                                        timestamp=device_event.timestamp)

        self.__collection_client.insert_one(device_info.model_dump())

        _logger.info("Stored ivs_car_version=%s for device=%s", ivscar_version, device_event.device_id)
        return True
