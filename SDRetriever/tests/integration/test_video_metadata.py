#  type: ignore
import pytest
import time

from base.aws.sqs import SQSController
from base.aws.s3 import S3Controller
from base.model.artifacts import parse_artifact, Artifact
from .mock_utils import load_files_rcc_chunks, S3File, get_s3_cloud_state, load_sqs_message, get_sqs_message, get_s3_file_content, get_sqs_message_artifact
from typing import Callable
from mypy_boto3_s3 import S3Client
import json


class TestVideoMetadataIngestion:
    @pytest.mark.integration()
    @pytest.mark.parametrize("rcc_files,input_sqs_message,expected_metadata,output_sqs_message",
                             [(
                                 get_s3_cloud_state("rcc_video_cloud_state.json"),
                                 get_sqs_message("interior_metadata_download_message.json"),
                                 get_s3_file_content("merged_interior_metadata.json"),
                                 get_sqs_message("interior_metadata_mdf_message.json"),
                             ),
                                 (
                                 get_s3_cloud_state("rcc_video_cloud_state.json"),
                                 get_sqs_message("training_metadata_download_message.json"),
                                 get_s3_file_content("merged_training_metadata.json"),
                                 get_sqs_message("training_metadata_mdf_message.json"),
                             )

                             ], ids=["integration_video_metadata_interior", "integration_video_metadata_training"])
    def test_success_video_metadata_ingestion(
            self,
            main_function: Callable,
            moto_s3_client: S3Client,
            rcc_bucket: str,
            devcloud_raw_bucket: str,
            download_queue_controller: SQSController,
            rcc_files: list[S3File],
            expected_metadata: bytes,
            input_sqs_message: str,
            output_sqs_message: str,
            mdf_queue_controller: SQSController):

        # GIVEN
        load_files_rcc_chunks(rcc_files, moto_s3_client, rcc_bucket)
        load_sqs_message(input_sqs_message, download_queue_controller)
        s3_controller = S3Controller(moto_s3_client)

        # WHEN
        main_function()

        # Post processing
        artifact = get_sqs_message_artifact(mdf_queue_controller, 1)

        result_bucket, key = s3_controller.get_s3_path_parts(artifact.s3_path)
        downloaded_metadata = s3_controller.download_file(devcloud_raw_bucket, key)

        parsed_devcloud_metadata = json.loads(downloaded_metadata.decode())
        parsed_expected_metadata = json.loads(expected_metadata.decode())

        # THEN
        assert download_queue_controller.get_message(0) is None
        assert parsed_expected_metadata == parsed_devcloud_metadata
        assert artifact == parse_artifact(output_sqs_message)
        assert result_bucket == devcloud_raw_bucket
