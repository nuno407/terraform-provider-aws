""" Models for the sanitizer """
from datetime import datetime
from pydantic import BaseModel


class DeviceInformation(BaseModel):
    """ Model for the device information """
    ivscar_version: str
    timestamp: datetime
    tenant_id: str
    device_id: str
