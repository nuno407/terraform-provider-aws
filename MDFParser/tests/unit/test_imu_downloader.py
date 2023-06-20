import pytest
from unittest.mock import ANY, Mock
from mdfparser.imu.downloader import IMUDownloader
from base.aws.container_services import ContainerServices
from mypy_boto3_s3 import S3Client
import pandas as pd
import numpy as np
import os


__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


def helper_df_imu_df(file_name: str) -> pd.DataFrame:
    file_path = os.path.join(__location__, "test_assets", "imu_data", file_name)
    df = pd.read_csv(file_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def helper_load_imu_file(file_name: str) -> bytes:
    file_path = os.path.join(__location__, "test_assets", "imu_data", file_name)
    return open(file_path, "rb").read()


class TestIMUDownloader:

    @pytest.fixture
    def imu_downloader(container_services_mock: ContainerServices, s3_client: S3Client) -> IMUDownloader:
        return IMUDownloader(container_services_mock, s3_client)

    @pytest.mark.unit
    @pytest.mark.parametrize("imu_bytes, expected_df",
                             [(helper_load_imu_file("datanauts_DATANAUTS_DEV_02_TrainingRecorder_1680541729312_1680541745612_imu.csv"),
                               helper_df_imu_df("datanauts_DATANAUTS_DEV_02_TrainingRecorder_1680541729312_1680541745612_imu_parsed.csv"),
                               ),
                                 (helper_load_imu_file("datanauts_ARTIFICAL_DATA_V2_TrainingRecorder_1680541729312_1680541745613_imu.csv"),
                                  helper_df_imu_df("datanauts_ARTIFICAL_DATA_V2_TrainingRecorder_1680541729312_1680541745613_imu_parsed.csv"),
                                  )],
                             ids=["test_imu_v1",
                                  "test_imu_v2"])
    def test_download(
            container_services_mock: ContainerServices,
            imu_bytes: bytes,
            expected_df: ANY,
            imu_downloader: IMUDownloader):
        # GIVEN
        container_services_mock.download_file = Mock(return_value=imu_bytes)
        mock_download = "s3://bucket/tenant/mock_file.csv"

        # WHEN
        result_df = imu_downloader.download(mock_download)

        # THEN
        container_services_mock.download_file.assert_called_once_with(ANY, "bucket", "tenant/mock_file.csv")
        pd.testing.assert_frame_equal(
            expected_df.reset_index(drop=True),
            result_df.reset_index(drop=True),
            check_dtype=False,
            check_exact=False)
