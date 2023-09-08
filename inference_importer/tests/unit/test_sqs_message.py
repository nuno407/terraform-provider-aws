""" Test SQSMessage Class """
import pytest
from datetime import datetime, timezone
from inference_importer.sqs_message import SQSMessage

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods


def _sqs_message_helper(transform_job_name, transform_job_status, model_name,
                        s3_output_path, instance_type, creation_time,
                        voxel_dataset_name, datasource_s3_bucket_name):
    return {"Body": str({
        "detail": {
            "TransformJobName": transform_job_name,
            "TransformJobStatus": transform_job_status,
            "ModelName": model_name,
            "TransformOutput": {
                "S3OutputPath": s3_output_path,
            },
            "TransformResources": {
                "InstanceType": instance_type,
            },
            "CreationTime": creation_time,
            "Tags": {
                "voxel_dataset_name": voxel_dataset_name,
                "datasource_s3_bucket_name": datasource_s3_bucket_name
            }
        }
    })
    }


@pytest.mark.unit
class TestSQSMessage():

    @pytest.mark.parametrize("sqs_message,expected_sqs_message", [
        (
            _sqs_message_helper("mock_transform_job_name", "mock_transform_job_status", "mock_model_name",
                                "mock_s3_output_path", "mock_instance_type", 1692855773000,
                                "mock_voxel_dataset_name", "mock_datasource_s3_bucket_name"),
            SQSMessage(transform_job_name="mock_transform_job_name",
                       transform_job_status="mock_transform_job_status",
                       transform_job_datetime=datetime.fromtimestamp(1692855773, tz=timezone.utc),
                       transform_job_output_path="mock_s3_output_path",
                       source_bucket_name="mock_datasource_s3_bucket_name",
                       dataset_name="mock_voxel_dataset_name",
                       model_name="mock_model_name",
                       instance_type="mock_instance_type")
        )
    ]
    )
    def test_from_raw_sqs_message(self, sqs_message, expected_sqs_message: SQSMessage):
        test_sqs_message = SQSMessage.from_raw_sqs_message(sqs_message)

        assert test_sqs_message.transform_job_name == expected_sqs_message.transform_job_name
        assert test_sqs_message.transform_job_status == expected_sqs_message.transform_job_status
        assert test_sqs_message.transform_job_datetime == expected_sqs_message.transform_job_datetime
        assert test_sqs_message.transform_job_output_path == expected_sqs_message.transform_job_output_path
        assert test_sqs_message.source_bucket_name == expected_sqs_message.source_bucket_name
        assert test_sqs_message.dataset_name == expected_sqs_message.dataset_name
        assert test_sqs_message.model_name == expected_sqs_message.model_name
        assert test_sqs_message.instance_type == expected_sqs_message.instance_type
