"""Models to be used by other modules"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class RCCS3SearchParams:
    """Parameters for RCC S3 search functions"""
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime
