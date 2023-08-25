#  type: ignore
import pytest
import time

from base.aws.sqs import SQSController
from base.aws.s3 import S3Controller
from base.model.artifacts import parse_artifact, Artifact, SnapshotArtifact
from .mock_utils import load_files_rcc_chunks, S3File, get_s3_cloud_state, load_sqs_message, get_sqs_message, get_local_content_from_s3_path, get_sqs_message_artifact, get_s3_file_content
from typing import Callable
from mypy_boto3_s3 import S3Client
from sdretriever.main import deserialize
import json


class TestSnapshotMetadataIngestion:
    @pytest.mark.integration()
    @pytest.mark.parametrize("rcc_files,input_sqs_message,metadata_sqs_message",
                             [(
                                 get_s3_cloud_state("snapshot_cloud_state.json"),
                                 get_sqs_message("snapshot_metadata_download.json"),
                                 get_sqs_message("snapshot_metadata_metadata.json"),
                             )

                             ], ids=["integration_snapshot_metadata"])
    def test_success_snapshot_metadata_ingestion(
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
        expected_snapshot_metadata = json.loads(get_local_content_from_s3_path(expected_artifact.s3_path).decode())
        s3_controller = S3Controller(moto_s3_client)

        # WHEN
        main_function()

        # Post processing
        result_artifact = get_sqs_message_artifact(metadata_queue_controller, 1)

        result_bucket, key = s3_controller.get_s3_path_parts(result_artifact.s3_path)
        result_snapshot_metadata = json.loads(s3_controller.download_file(devcloud_raw_bucket, key).decode())

        # THEN
        assert download_queue_controller.get_message(0) is None
        assert result_snapshot_metadata == expected_snapshot_metadata
        assert result_artifact == expected_artifact
        assert result_bucket == devcloud_raw_bucket
