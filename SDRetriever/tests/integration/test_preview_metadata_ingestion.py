#  type: ignore
import pytest
import time

from base.aws.sqs import SQSController
from base.aws.s3 import S3Controller
from base.model.artifacts import parse_artifact, Artifact
from .mock_utils import load_files_rcc, S3File, get_s3_cloud_state, load_sqs_message, get_sqs_message, get_s3_file_content
from typing import Callable
from mypy_boto3_s3 import S3Client
from sdretriever.main import deserialize
import json


class TestPreviewMetadataIngestion:
    @pytest.mark.integration()
    @pytest.mark.parametrize("rcc_files,input_sqs_message,expected_metadata,output_sqs_message",
                             [(
                                 get_s3_cloud_state("rcc_cloud_state.json"),
                                 get_sqs_message("preview_signals_message_download.json"),
                                 get_s3_file_content("preview_recorder_merged.json"),
                                 get_sqs_message("preview_signals_message_hq.json"),
                             )

                             ], ids=["integration_test_1"])
    def test_success_preview_ingestion(
            self,
            main_function: Callable,
            moto_s3_client: S3Client,
            moto_sqs_client: S3Client,
            rcc_bucket: str,
            devcloud_temporary_bucket: str,
            download_queue: str,
            rcc_files: list[S3File],
            expected_metadata: str,
            input_sqs_message: str,
            output_sqs_message: str,
            selector_queue: str):

        # GIVEN
        load_files_rcc(rcc_files, moto_s3_client, rcc_bucket)
        load_sqs_message(input_sqs_message, moto_sqs_client, download_queue)

        selector_sqs_controller = SQSController(selector_queue, moto_sqs_client)
        download_sqs_controller = SQSController(download_queue, moto_sqs_client)
        s3_controller = S3Controller(moto_s3_client)

        # WHEN
        main_function()
        time.sleep(1)

        # Post processing
        hq_message = selector_sqs_controller.get_message()
        hq_parsed_message = json.loads(deserialize(hq_message["Body"]))
        artifact: Artifact = parse_artifact(hq_parsed_message)

        result_bucket, key = s3_controller.get_s3_path_parts(artifact.s3_path)
        downloaded_metadata = s3_controller.download_file(devcloud_temporary_bucket, key)

        parsed_devcloud_metadata = json.loads(downloaded_metadata.decode())
        parsed_expected_metadata = json.loads(expected_metadata.decode())

        # THEN
        assert download_sqs_controller.get_message(0) == None
        assert parsed_expected_metadata == parsed_devcloud_metadata
        assert hq_parsed_message == json.loads(output_sqs_message)
        assert result_bucket == devcloud_temporary_bucket
