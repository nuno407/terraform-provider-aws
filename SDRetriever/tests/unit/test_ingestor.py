from unittest.mock import Mock
from unittest.mock import patch

import pytest

from sdretriever.ingestor import Ingestor


@pytest.mark.unit
@pytest.mark.usefixtures("container_services", "s3_client", "sqs_client", "sts_helper")
class TestIngestor:

    @pytest.fixture(params=[{'KeyCount': 0, 'Contents': []}, {'KeyCount': 1,
                    'Contents': [{'Key': '/This/is/a/path', 'Size': 12345}]}])
    def response(self, request) -> dict:
        # uses two params, one for a case where it finds one matching item in the
        # bucket, and another one for when it doesn't
        return request.param

    @patch("sdretriever.ingestor.ContainerServices")
    def test_check_if_exists(
            self,
            mock_container_services,
            response,
            container_services,
            s3_client,
            sqs_client,
            sts_helper):
        self.obj = Ingestor(container_services, s3_client,
                            sqs_client, sts_helper)
        # Case where it finds one matching item in the bucket
        mock_container_services.list_s3_objects.return_value = response
        s3_path = "Debug_Lync/TrainingMultiSnapshot_TrainingMultiSnapshot-0015ab73-c9f0-442f-adb1-31cfdf0d886e_3_1651646840.jpeg"
        bucket = "qa-rcd-raw-video-files"
        exists, _ = self.obj.check_if_s3_rcc_path_exists(
            s3_path, bucket)
        assert exists == bool(response['KeyCount'])
