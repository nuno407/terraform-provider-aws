from unittest.mock import Mock, call
from unittest.mock import patch, ANY
from datetime import datetime
import pytest
from typing import Optional

from sdretriever.ingestor import Ingestor


@pytest.mark.unit
@pytest.mark.usefixtures("container_services", "s3_client", "sqs_client",
                         "sts_helper", "list_s3_objects")
class TestIngestor:

    @pytest.fixture(params=[{'KeyCount': 0, 'Contents': []}, {'KeyCount': 1,
                    'Contents': [{'Key': '/This/is/a/path', 'Size': 12345}]}])
    def response(self, request) -> dict:
        # uses two params, one for a case where it finds one matching item in the
        # bucket, and another one for when it doesn't
        return request.param

    @pytest.fixture
    def expected_rcc_folders(self):
        return [
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=16/',
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=09/hour=16/',
            'datanauts/DATANAUTS_DEV_01/year=2022/month=08/day=19/hour=12/'
        ]

    @pytest.fixture
    def obj(self, container_services, s3_client, sqs_client, sts_helper):
        return Ingestor(container_services, s3_client, sqs_client, sts_helper)

    @patch("sdretriever.ingestor.ContainerServices")
    def test_check_if_exists(
            self,
            mock_container_services,
            response,
            obj):
        # Case where it finds one matching item in the bucket
        mock_container_services.list_s3_objects.return_value = response
        s3_path = "Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-0015ab73-c9f0-442f-adb1-31cfdf0d886e_3_1651646840.jpeg"
        bucket = "qa-rcd-raw-video-files"
        exists, _ = obj.check_if_s3_rcc_path_exists(
            s3_path, bucket)
        assert exists == bool(response['KeyCount'])

    @patch("sdretriever.ingestor.ContainerServices")
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
            self,
            mock_container_services,
            obj,
            s3_client,
            list_s3_objects,
            paths_reference,
            wanted_result,
            start_datetime_reference,
            end_datetime_reference):

        list_s3_objects_func, path_list = list_s3_objects
        path_list.extend(paths_reference)
        mock_container_services.list_s3_objects.side_effect = list_s3_objects_func

        folder_reference = "TEST_TENANT/TEST_DEVICE_ID/"
        bucket_reference = "UNUSED"

        paths = obj._discover_s3_subfolders(folder_reference, bucket_reference, s3_client,
                                            start_time=start_datetime_reference, end_time=end_datetime_reference)

        assert set(paths) == set(wanted_result)

    @pytest.mark.unit
    @pytest.mark.parametrize("list_objects_response,prefix,extensions,raise_val", [
        (
            [(True, {
                "Contents": [
                    {
                        "Key": "mocked_path/video.mp4.json"
                    },
                    {
                        "Key": "mocked_path/video.mp4"
                    }
                ]
            }), (False, {}), (False, {})],
            "video",
            [".json"],
            None
        ),
        (
            [(True, {
                "Contents": [
                    {
                        "Key": "mocked_path/video.mp4.json"
                    },
                    {
                        "Key": "mocked_path/video.mp4"
                    }
                ]
            }), (False, {}), (False, {})],
            "video",
            [".txt"],
            FileNotFoundError
        )
    ])
    def test_get_file_in_rcc(self,
                             obj: Ingestor,
                             expected_rcc_folders: list[str],
                             list_objects_response: tuple[bool, list[dict]],
                             prefix: str,
                             extensions: list[str],
                             raise_val: Optional[Exception]):

        bucket = "test"
        tenant = "test"
        device_id = "test"
        start_time = datetime.now()
        end_time = datetime.now()

        result = b"mock_bytes"

        obj._discover_s3_subfolders = Mock(return_value=expected_rcc_folders)
        obj.check_if_s3_rcc_path_exists = Mock(side_effect=list_objects_response)
        obj.CS.download_file = Mock(return_value=result)

        if not raise_val:
            rtn = obj.get_file_in_rcc(bucket, tenant, device_id, prefix, start_time, end_time, extensions)

            obj._discover_s3_subfolders.assert_called_once_with(
                f'{tenant}/{device_id}', bucket, ANY, start_time, end_time)
            obj.check_if_s3_rcc_path_exists.assert_called()

            assert rtn == result

        else:
            with pytest.raises(raise_val):
                obj.get_file_in_rcc(bucket, tenant, device_id, prefix, start_time, end_time, extensions)
                assert True
