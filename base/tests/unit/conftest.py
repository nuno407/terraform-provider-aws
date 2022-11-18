""" Conftest."""
from datetime import datetime
from unittest.mock import Mock

import pytest


# pylint: disable=missing-function-docstring
@pytest.fixture
def container_services() -> Mock:
    container_service = Mock()
    container_service.upload_file = Mock()
    container_service.raw_s3 = "dev-rcd-raw-video-files"
    container_service.sdr_folder = {"debug": "Debug_Lync/",
                                    "fut2": "FUT2/", "driver_pr": "Driver-PR/"}
    container_service.get_kinesis_clip = Mock(return_value=(b"These are some video bytes", datetime.fromtimestamp(
        1657297040802 / 1000.0), datetime.fromtimestamp(1657297074110 / 1000.0)))
    container_service.sqs_queues_list = {
        "SDM": "dev-terraform-queue-s3-sdm",
        "Anonymize": "dev-terraform-queue-anonymize",
        "API_Anonymize": "dev-terraform-queue-api-anonymize",
        "ACAPI": "",
        "CHC": "dev-terraform-queue-chc",
        "API_CHC": "dev-terraform-queue-api-chc",
        "SDRetriever": "dev-terraform-queue-download",
        "Selector": "dev-terraform-queue-selector",
        "HQ_Selector": "dev-terraform-queue-hq-request",
        "Metadata": "dev-terraform-queue-metadata",
        "Output": "dev-terraform-queue-output"
    }
    container_service.RCC_S3_CLIENT = s3_client
    container_service.rcc_info = {"s3_bucket": "rcc-dev-device-data"}
    return container_service


@pytest.fixture
def s3_client():
    return Mock()


@pytest.fixture
def sqs_client():
    return Mock()


@pytest.fixture
def rcc_bucket():
    return "rcc-prod-device-data"


@pytest.fixture
def rcc_s3_list_prefix():
    return "ridecare_companion_trial/rc_srx_prod_5cd8076d1cbddd483603db282ff9cc00cb76909f/year=2022/month=11/day=01/hour=16/"  # pylint: disable=line-too-long
