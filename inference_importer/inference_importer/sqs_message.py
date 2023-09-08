"""SQS Message module"""

import json
from datetime import datetime, timezone
from dataclasses import dataclass

from base.aws.container_services import ContainerServices

_logger = ContainerServices.configure_logging(__name__)


@dataclass
class SQSMessage():  # pylint:disable=too-many-instance-attributes
    """
    Class holing information of a parsed SQS message
    """
    transform_job_name: str
    transform_job_status: str
    transform_job_datetime: datetime
    transform_job_output_path: str
    source_bucket_name: str
    dataset_name: str
    model_name: str
    instance_type: str

    # Logs the message

    def print_message(self):
        """
        Print message attributes to log.
        """
        _logger.info("Transform Job Name: %s", self.transform_job_name)
        _logger.info("Transform Job Status: %s", self.transform_job_status)
        _logger.info("Transform Job Datetime: %s", self.transform_job_datetime)
        _logger.info("Transform Job Output Path: %s", self.transform_job_output_path)
        _logger.info("Source Bucket Name: %s", self.source_bucket_name)
        _logger.info("Dataset Name: %s", self.dataset_name)
        _logger.info("Model Name: %s", self.model_name)
        _logger.info("Instance Type: %s", self.instance_type)

    @classmethod
    def from_raw_sqs_message(cls, sqs_message):
        """
        Creates an SQS Message object from parsing a raw message

        :param sqs_message: Raw message to parse
        """

        body = sqs_message["Body"].replace("\'", "\"")
        sqs_body = json.loads(body)

        transform_job_name = sqs_body["detail"]["TransformJobName"]
        transform_job_status = sqs_body["detail"]["TransformJobStatus"]
        transform_job_datetime = datetime.fromtimestamp(sqs_body["detail"]["CreationTime"] / 1000.0, tz=timezone.utc)
        transform_job_output_path = sqs_body["detail"]["TransformOutput"]["S3OutputPath"]
        source_bucket_name = sqs_body["detail"]["Tags"]["datasource_s3_bucket_name"]
        dataset_name = sqs_body["detail"]["Tags"]["voxel_dataset_name"]
        model_name = sqs_body["detail"]["ModelName"]
        instance_type = sqs_body["detail"]["TransformResources"]["InstanceType"]

        message_to_return = SQSMessage(transform_job_name,
                                       transform_job_status,
                                       transform_job_datetime,
                                       transform_job_output_path,
                                       source_bucket_name,
                                       dataset_name,
                                       model_name,
                                       instance_type)

        message_to_return.print_message()

        return message_to_return
