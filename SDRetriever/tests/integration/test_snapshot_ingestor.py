#  type: ignore
import pytest
import time

from base.aws.sqs import SQSController
from base.aws.s3 import S3Controller
from base.model.artifacts import parse_artifact, Artifact, SnapshotArtifact
from .mock_utils import load_files_rcc_chunks, S3File, get_s3_cloud_state, load_sqs_message, get_sqs_message, get_local_content_from_s3_path, get_sqs_message_artifact
from typing import Callable
from mypy_boto3_s3 import S3Client
from sdretriever.main import deserialize
import json


class TestTrainingSnapshotIngestion:
    @pytest.mark.integration()
    @pytest.mark.parametrize("rcc_files,input_sqs_message,metadata_sqs_message",
                             [(
                                 get_s3_cloud_state("snapshot_cloud_state.json"),
                                 get_sqs_message("training_snapshot_recorder_message_download.json"),
                                 get_sqs_message("training_snapshot_recorder_message_metadata.json"),
                             ),
                                 # Test when timestamp == last_modified_timestamp
                                 (
                                 get_s3_cloud_state("snapshot_cloud_state_2.json"),
                                 get_sqs_message("training_snapshot_recorder_message_download.json"),
                                 get_sqs_message("training_snapshot_recorder_message_metadata.json"),
                             )

                             ], ids=["integration_training_snapshot_1", "integration_training_snapshot_2"])
    def test_success_preview_ingestion(
            self,
            main_function: Callable,
            moto_s3_client: S3Client,
            rcc_bucket: str,
            devcloud_raw_bucket: str,
            download_queue_controller: SQSController,
            rcc_files: list[S3File],
            input_sqs_message: str,
            metadata_sqs_message: str,
            metadata_queue_controller: SQSController):

        # GIVEN
        load_files_rcc_chunks(rcc_files, moto_s3_client, rcc_bucket)
        load_sqs_message(input_sqs_message, download_queue_controller)

        expected_artifact: SnapshotArtifact = parse_artifact(metadata_sqs_message)
        expected_snapshot = get_local_content_from_s3_path(expected_artifact.uuid)
        s3_controller = S3Controller(moto_s3_client)

        # WHEN
        main_function()

        # Post processing
        result_artifact = get_sqs_message_artifact(metadata_queue_controller, 1)

        result_bucket, key = s3_controller.get_s3_path_parts(result_artifact.s3_path)
        result_snapshot = s3_controller.download_file(devcloud_raw_bucket, key)

        # THEN
        assert download_queue_controller.get_message(0) is None
        assert result_snapshot == expected_snapshot
        assert result_artifact == expected_artifact
        assert result_bucket == devcloud_raw_bucket
