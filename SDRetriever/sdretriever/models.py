"""Models to be used by other modules"""

from dataclasses import dataclass
from datetime import datetime
from typing import Generator
from base.model.artifacts import RecorderType


@dataclass
class S3Object:
    """Data from an S3 bucket"""
    data: bytes


@dataclass
class S3ObjectDevcloud(S3Object):
    """Data from an S3 bucket"""
    filename: str
    tenant: str


@dataclass
class S3ObjectRCC(S3Object):
    """Data from an S3 bucket"""
    s3_key: str
    bucket: str


@dataclass
class RCCS3SearchParams:
    """Parameters for RCC S3 search functions"""
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime


@dataclass
class ChunkDownloadParamsByPrefix:
    """Parameters for RCC S3 chunk ingestion functions"""
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime
    files_prefix: list[str]
    suffixes: list[str]

    def get_search_parameters(self) -> RCCS3SearchParams:
        """Return RCCS3SearchParams"""
        return RCCS3SearchParams(
            device_id=self.device_id,
            tenant=self.tenant,
            start_search=self.start_search,
            stop_search=self.stop_search
        )


@dataclass
class ChunkDownloadParamsByID:  # pylint: disable=too-many-instance-attributes
    """Parameters for RCC S3 chunk ingestion functions"""
    recorder: RecorderType
    recording_id: str
    chunk_ids: list[int]
    device_id: str
    tenant: str
    start_search: datetime
    stop_search: datetime
    suffixes: list[str]

    def get_chunks_prefix(self) -> Generator[str, None, None]:
        """Returns prefix of the files chunks

        Yields:
            Generator[str, None, None]: List of prefix
        """
        for chunk_id in self.chunk_ids:
            yield f"{self.recorder.value}_{self.recording_id}_{chunk_id}"

    def get_search_parameters(self) -> RCCS3SearchParams:
        """Return RCCS3SearchParams"""
        return RCCS3SearchParams(
            device_id=self.device_id,
            tenant=self.tenant,
            start_search=self.start_search,
            stop_search=self.stop_search
        )
