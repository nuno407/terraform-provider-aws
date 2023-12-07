#  type: ignore
import pytest
import time
import logging
from base.aws.sqs import SQSController
from base.aws.s3 import S3Controller
from base.model.artifacts import parse_artifact, S3VideoArtifact
from .mock_utils import load_sqs_message, get_sqs_message, load_files, get_local_content_from_s3_path, get_sqs_message_artifact
from typing import Callable
from mypy_boto3_s3 import S3Client
from typing import Optional


class TestS3VideoIngestion:
    @pytest.mark.integration()
    @pytest.mark.parametrize("input_sqs_message,metadata_sqs_message,selector_sqs_message",
                             [(
                                 get_sqs_message("training_recorder_message_download.json"),
                                 get_sqs_message("training_recorder_message_metadata.json"),
                                 None,
                             ), (
                                 get_sqs_message("interior_recorder_message_download.json"),
                                 get_sqs_message("interior_recorder_message_metadata_hq.json"),
                                 None
                             )
                             ], ids=["integration_s3_training_video", "integration_s3_interior_video"])
    def test_success_s3_ingestion(
            self,
            main_function: Callable,
            moto_s3_client: S3Client,
            devcloud_raw_bucket: str,
            input_sqs_message: str,
            selector_sqs_message: Optional[str],
            metadata_sqs_message: str,
            download_queue_controller: SQSController,
            selector_queue_controller: SQSController,
            metadata_queue_controller: SQSController):

        # GIVEN
        expected_metadata_artifact: S3VideoArtifact = parse_artifact(metadata_sqs_message)

        input_message_artifact: S3VideoArtifact = parse_artifact(input_sqs_message)
        input_file_rcc_s3_path = input_message_artifact.rcc_s3_path

        expected_video = get_local_content_from_s3_path(input_file_rcc_s3_path)

        load_sqs_message(input_sqs_message, download_queue_controller)
        load_files([input_file_rcc_s3_path], moto_s3_client)

        s3_controller = S3Controller(moto_s3_client)

        # WHEN
        main_function()

        # Post processing
        result_metadata_artifact = get_sqs_message_artifact(metadata_queue_controller, 1)
        result_selector_artifact = get_sqs_message_artifact(selector_queue_controller, 1)

        _, key = s3_controller.get_s3_path_parts(result_metadata_artifact.s3_path)
        result_video = s3_controller.download_file(devcloud_raw_bucket, key)

        # THEN
        if selector_sqs_message is not None:
            expected_metadata_artifact: S3VideoArtifact = parse_artifact(selector_sqs_message)
            assert result_selector_artifact == expected_metadata_artifact
        else:
            assert result_selector_artifact is None

        assert download_queue_controller.get_message(0) is None
        assert result_video == expected_video
        assert expected_metadata_artifact == result_metadata_artifact
