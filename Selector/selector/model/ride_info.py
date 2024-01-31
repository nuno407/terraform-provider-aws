"""RideInfo module"""
from datetime import datetime
from dataclasses import dataclass
from selector.model.preview_metadata import PreviewMetadata


@dataclass
class RideInfo:
    """
    Ride information

    - start_ride: represents the timestamp when the ride started recieved from the RCC
    - end_ride: represents the timestamp when the ride started recieved from the RCC

    This should be used to request a training data instead of the timestamps within the metadata.
    """
    preview_metadata: PreviewMetadata
    start_ride: datetime
    end_ride: datetime
