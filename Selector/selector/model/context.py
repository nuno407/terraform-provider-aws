"""Context module"""
from dataclasses import dataclass

from selector.model.ride_info import RideInfo


@dataclass
class Context:
    """Object containing all values and data sources for a Rule to operate its logic"""
    ride_info: RideInfo
    tenant_id: str
    device_id: str
