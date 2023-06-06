# type: ignore
""" Test S3 finder """
from datetime import datetime
from typing import Any, Callable, Optional
from unittest.mock import Mock, patch

import pytest
from mypy_boto3_s3 import S3Client
from sdretriever.s3_finder_rcc import S3FinderRCC


@pytest.fixture
def list_s3_objects() -> Callable[[str, str, S3Client, Optional[str]], Any]:
    paths_reference: list[str] = []

    def mock_list_s3_objects(s3_path: str, bucket: str, s3_client: S3Client, delimiter: Optional[str] = None):
        result_set = set()
        for reference in paths_reference:
            if reference.startswith(s3_path):
                # Strip the path to contain until the next the delimiter
                pos_delimiter = reference[len(s3_path):].find(delimiter)
                if pos_delimiter != -1:
                    final_pos = 1 + pos_delimiter + len(s3_path)
                else:
                    final_pos = len(reference)

                result_set.add(reference[:final_pos])

        result = {}
        result['CommonPrefixes'] = []
        for each in result_set:
            result['CommonPrefixes'].append({'Prefix': each})
        return result

    return mock_list_s3_objects, paths_reference


@pytest.mark.unit
@pytest.mark.parametrize("paths_reference,wanted_result,start_datetime_reference,end_datetime_reference", [
    # Success
    (
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=09/day=30/hour=20/TEST_ID_TEST_TYPE",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_ID_TEST_TYPE"
        ],
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/"
        ],
        datetime(year=2022, month=8, day=20, hour=1),
        datetime(year=2022, month=10, day=1, hour=1)
    ),
    (
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=09/day=30/hour=20/TEST_ID_TEST_TYPE",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=21/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=02/TEST_ID_TEST_TYPE",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=03/TEST_ID_TEST_TYPE"
        ],
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=09/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=01/hour=01/"
        ],
        datetime(
            year=2022, month=9, day=30, hour=22, minute=10),
        datetime(year=2022, month=10, day=1, hour=1)
    ),
    (
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=05/day=25/hour=20/",
            "TEST_TENANT/TEST_DEVICE_ID2/year=2022/month=10/day=30/hour=20/TEST_ID_TEST_TYPE1",
            "TEST_TENANT2/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=21/TEST_ID_TEST_TYPE2",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=22/TEST_ID_TEST_TYPE3",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/TEST_ID_TEST_TYPE4",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=00/TEST_ID_TEST_TYPE5",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=01/TEST_ID_TEST_TYPE6",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=02/TEST_ID_TEST_TYPE7",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=03/TEST_ID_TEST_TYPE8",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=03/day=01/hour=03/TEST_ID_TEST_TYPE8"
        ],
        [
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=22/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=10/day=30/hour=23/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=00/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2022/month=11/day=01/hour=01/",
            "TEST_TENANT/TEST_DEVICE_ID/year=2023/month=02/day=28/hour=02/",
        ],
        datetime(
            year=2022, month=10, day=30, hour=22, minute=10),
        datetime(year=2023, month=2, day=28, hour=2)
    )
])
def test_discover_s3_subfolders(
        list_s3_objects,
        paths_reference,
        wanted_result,
        start_datetime_reference,
        end_datetime_reference,
        container_services,
        s3_finder,
        s3_client):

    list_s3_objects_func, path_list = list_s3_objects
    path_list.extend(paths_reference)
    container_services.list_s3_objects.side_effect = list_s3_objects_func

    folder_reference = "TEST_TENANT/TEST_DEVICE_ID/"
    bucket_reference = "UNUSED"

    s3_finder = S3FinderRCC(bucket_reference, s3_client, container_services)

    paths = s3_finder.discover_s3_subfolders(folder_reference,
                                             start_time=start_datetime_reference, end_time=end_datetime_reference)

    assert set(paths) == set(wanted_result)
