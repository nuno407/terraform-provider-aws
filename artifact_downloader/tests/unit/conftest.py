"""Conftest"""
import pytest
from unittest.mock import Mock
from artifact_downloader.post_processor import FFProbeExtractorPostProcessor
from artifact_downloader.chc_synchronizer import ChcSynchronizer
from artifact_downloader.request_factory import RequestFactory
from artifact_downloader.s3_downloader import S3Downloader


@pytest.fixture
def mock_post_processor() -> FFProbeExtractorPostProcessor:
    """Mock for post processor"""
    return Mock()


@pytest.fixture
def mock_chc_syncronizer() -> ChcSynchronizer:
    """Mock for chc sycronizer"""
    return Mock()


@pytest.fixture
def mock_request_factory() -> RequestFactory:
    """Mock for RequestFactory"""
    return Mock()


@pytest.fixture
def mock_s3_downloader() -> S3Downloader:
    """Mock for S3Downloader"""
    return Mock()
