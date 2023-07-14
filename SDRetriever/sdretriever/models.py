"""Models to be used by other modules"""

from dataclasses import dataclass
from datetime import datetime
from base.model.artifacts import RecorderType
from typing import Generator


@dataclass
class RCCS3SearchParams:
    """Parameters for RCC S3 search functions"""
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime


@dataclass
class S3ObjectDevcloud:
    """Data from an S3 bucket"""
    data: bytes
    filename: str
    tenant: str


@dataclass
class S3ObjectRCC:
    """Data from an S3 bucket"""
    data: bytes
    s3_key: str
    bucket: str


@dataclass
class ChunkDownloadParams:
    """Parameters for RCC S3 chunk ingestion functions"""
    recorder: RecorderType
    recording_id: str
    chunk_ids: list[int]
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime
    suffix: str

    def get_chunks_prefix(self) -> Generator[str, None, None]:
        for chunk_id in self.chunk_ids:
            yield f"{self.recorder.value}_{self.recording_id}_{chunk_id}"

    def get_search_parameters(self) -> RCCS3SearchParams:
        return RCCS3SearchParams(
            device_id=self.device_id,
            tenant=self.tenant,
            start_search=self.start_search,
            stop_search=self.stop_search
        )
