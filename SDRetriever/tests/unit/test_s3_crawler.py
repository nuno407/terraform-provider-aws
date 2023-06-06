# type: ignore
""" Test S3 finder """
from datetime import datetime
from typing import Callable, Optional
from unittest.mock import Mock, call
import re
import pytest
from sdretriever.s3_crawler_rcc import S3CrawlerRCC
from sdretriever.models import RCCS3SearchParams
from sdretriever.s3_finder_rcc import S3FinderRCC
from base.aws.s3 import S3Controller
from base.aws.model import S3ObjectInfo

DUMMY_DATE = datetime.now()
COMMON_PATH = "datanauts/DATANAUTS_DEV_01/year=2023/month=06/day=12/hour=07"
COMMON_TRAINING_NAME = "TrainingRecorder_TrainingRecorder-502a694c-302a-44b7-ac56-aba717fe8f6d"
COMMON_INTERIOR_NAME = "InteriorRecorder_InteriorRecorder-834afe5f-5857-41ec-b5d7-4b77407721ed"
COMMON_PREVIEW_NAME = "InteriorRecorderPreview_InteriorRecorderPreview-56a27dd7-0ba6-42f9-8c5c-200ebb38d143"


@pytest.fixture
def s3_crawler(s3_controller_factory: S3Controller, rcc_bucket: str,
               s3_finder: S3FinderRCC) -> S3CrawlerRCC:
    return S3CrawlerRCC(s3_controller_factory, rcc_bucket, s3_finder)


def metadata_chunk_match(object_info: S3ObjectInfo) -> Optional[str]:
    file_key = object_info.key

    if not (file_key.endswith(".json") or file_key.endswith(".zip")):
        return None

    recording_regex = re.compile(r"hour=\d{2}\/(.+?\.\w+)").search(file_key)
    if not recording_regex:
        return None
    return recording_regex.group(1)


def helper_create_s3_object(s3_key) -> S3ObjectInfo:
    return S3ObjectInfo(key=s3_key, date_modified=DUMMY_DATE, size=1)


def helper_s3_objects() -> set[S3ObjectInfo]:
    return {
        helper_create_s3_object(
            f"{COMMON_PATH}/DATANAUTS_DEV_01__2023-06-12T07.07.56__logfile_bluetoothservice.LAST_SHUTDOWN__4.txt_zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_1.mp4"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_1.mp4._stream1_20230606135528_0_imu_raw.csv.zip"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_1.mp4._stream1_20230606135528_0_metadata.json.zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_2.mp4"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_2.mp4._stream1_20230606135540_1_imu_raw.csv.zip"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_2.mp4._stream1_20230606135540_1_metadata.json.zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_INTERIOR_NAME}_1.mp4"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_INTERIOR_NAME}_1.mp4._stream2_20230606135528_0_metadata.json.zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_INTERIOR_NAME}_2.mp4"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_INTERIOR_NAME}_2.mp4._stream2_20230606135540_1_metadata.json.zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_1.jpeg"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_2.jpeg"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_1.jpeg._stream2_20230606135540_1_metadata.json.zip"),
        helper_create_s3_object(
            f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_2.jpeg._stream2_20230606135540_1_metadata.json.zip"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_3.jpeg"),
        helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_4.jpeg._stream2_20230606135540_1_metadata.json.zip")}


class TestS3Crawler:

    @pytest.mark.unit
    @pytest.mark.parametrize("prefix", [
        "",
        "TrainingRecorder"
    ])
    def test_list_all_objects(self,
                              prefix: str,
                              s3_crawler: S3CrawlerRCC,
                              s3_finder: S3FinderRCC,
                              s3_controller: S3Controller,
                              rcc_bucket: str):
        """
        Test list all objects

        Args:
            prefix (str): _description_
            s3_crawler (S3CrawlerRCC): _description_
            s3_finder (S3FinderRCC): _description_
            s3_controller (S3Controller): _description_
            rcc_bucket (str): _description_
        """

        # GIVEN
        discovered_paths = [
            f"{COMMON_PATH}/",
            "datanauts/DATANAUTS_DEV_01/year=2023/month=06/day=12/hour=08/",
            "datanauts/DATANAUTS_DEV_01/year=2023/month=06/day=12/hour=09",
        ]

        list_objects_calls = [
            call(f"{COMMON_PATH}/{prefix}", rcc_bucket),
            call(
                f"datanauts/DATANAUTS_DEV_01/year=2023/month=06/day=12/hour=08/{prefix}",
                rcc_bucket),
            call(
                f"datanauts/DATANAUTS_DEV_01/year=2023/month=06/day=12/hour=09/{prefix}",
                rcc_bucket),
        ]

        result_expected = [
            Mock(),
            Mock(),
            Mock(),
        ]

        tenant_id = "datanauts"
        device_id = "DATANAUTS_DEV_01"
        start_time = Mock()
        end_time = Mock()
        s3_finder.discover_s3_subfolders = Mock(return_value=discovered_paths)
        s3_controller.list_directory_objects = Mock(return_value=iter(result_expected))

        params = RCCS3SearchParams(device_id, tenant_id, start_time, end_time)

        # WHEN
        if prefix == "":
            result = s3_crawler.list_all_objects(params)
        else:
            result = s3_crawler.list_all_objects(params, prefix)

        # THEN
        assert list(result) == result_expected
        s3_finder.discover_s3_subfolders.assert_called_once_with(
            "datanauts/DATANAUTS_DEV_01/", start_time, end_time)
        s3_controller.list_directory_objects.assert_has_calls(list_objects_calls, any_order=True)

    @pytest.mark.unit
    @pytest.mark.parametrize("files_in_s3,required_files,match_function,expected_matches,expected_common_prefix",

                             [
                                 # Test all preview missing
                                 (
                                     helper_s3_objects(),
                                     {
                                         f"{COMMON_PREVIEW_NAME}_1.jpeg",
                                         f"{COMMON_PREVIEW_NAME}_3.jpeg"
                                     },
                                     metadata_chunk_match,
                                     {
                                         f"{COMMON_PREVIEW_NAME}_1.jpeg": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_1.jpeg._stream2_20230606135540_1_metadata.json.zip")
                                     },
                                     f"{COMMON_PREVIEW_NAME}_"
                                 ),

                                 # Test preview present
                                 (
                                     helper_s3_objects(),
                                     {
                                         f"{COMMON_PREVIEW_NAME}_1.jpeg",
                                         f"{COMMON_PREVIEW_NAME}_2.jpeg"
                                     },
                                     metadata_chunk_match,
                                     {
                                         f"{COMMON_PREVIEW_NAME}_1.jpeg": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_1.jpeg._stream2_20230606135540_1_metadata.json.zip"),
                                         f"{COMMON_PREVIEW_NAME}_2.jpeg": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_2.jpeg._stream2_20230606135540_1_metadata.json.zip")
                                     },
                                     f"{COMMON_PREVIEW_NAME}_"
                                 ),
                                 # Test preview metadata present and snapshot missing
                                 (
                                     helper_s3_objects(),
                                     {
                                         f"{COMMON_PREVIEW_NAME}_4.jpeg"
                                     },
                                     metadata_chunk_match,
                                     {
                                         f"{COMMON_PREVIEW_NAME}_4.jpeg": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_PREVIEW_NAME}_4.jpeg._stream2_20230606135540_1_metadata.json.zip")
                                     },
                                     f"{COMMON_PREVIEW_NAME}_4.jpeg"
                                 ),
                                 # Test training
                                 (
                                     helper_s3_objects(),
                                     {
                                         f"{COMMON_TRAINING_NAME}_1.mp4",
                                         f"{COMMON_TRAINING_NAME}_2.mp4"
                                     },
                                     None,
                                     {
                                         f"{COMMON_TRAINING_NAME}_1.mp4": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_1.mp4"),
                                         f"{COMMON_TRAINING_NAME}_2.mp4": helper_create_s3_object(f"{COMMON_PATH}/{COMMON_TRAINING_NAME}_2.mp4")
                                     },
                                     f"{COMMON_TRAINING_NAME}_"
                                 )

                             ]
                             )
    def test_search_files(self,
                          files_in_s3: list[str],
                          required_files: set[str],
                          match_function: Callable[[S3ObjectInfo],
                                                   Optional[S3ObjectInfo]],
                          expected_matches: dict[str,
                                                 S3ObjectInfo],
                          expected_common_prefix: str,
                          s3_crawler: S3CrawlerRCC):
        """
        Tests search files

        Args:
            files_in_s3 (list[str]): A files to be returned by the mock list_all_objects
            required_files (set[str]): The files that are to be passed to the function being tested
            match_function (Callable[[S3ObjectInfo], Optional[S3ObjectInfo]]): The function used to match the index
            expected_matches (dict[str, S3ObjectInfo]): The expected result of the search
            expected_common_prefix (str): The expected common prefix between all chunks
            s3_crawler (S3CrawlerRCC): The crawler
        """

        # GIVEN
        device_id: str = "mock_device"
        tenant: str = "mock_tenant"
        start_search: datetime = Mock()
        end_search: datetime = Mock()

        s3_crawler.list_all_objects = Mock(return_value=files_in_s3)

        params = RCCS3SearchParams(tenant, device_id, start_search, end_search)

        # WHEN
        if match_function:
            result = s3_crawler.search_files(
                required_files,
                params,
                match_function)
        else:
            result = s3_crawler.search_files(
                required_files, params)

        # THEN
        s3_crawler.list_all_objects.assert_called_once_with(
            params, expected_common_prefix)

        assert result == expected_matches
