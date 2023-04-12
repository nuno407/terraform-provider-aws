""" This module contains the S3Interaction class. """
import re
from mypy_boto3_s3 import S3Client
from base.aws.container_services import ContainerServices
from kink import inject


@inject
class S3Interaction:  # pylint: disable=too-few-public-methods
    """ This class contains methods to interact with S3. """

    def __init__(self, container_services: ContainerServices, s3_client: S3Client) -> None:
        """
        Initializes the S3Interaction class.

        Args:
            container_services (ContainerServices): The container services.
        """
        self.s3_regx = re.compile(r"^s3://([^/]+)/(.*)$")
        self._s3_client = s3_client
        self._container_services = container_services

    def _get_s3_path(self, raw_path: str) -> tuple[str, str]:
        """
        Returns a bucket and key from an s3 path.

        Args:
            raw_path (str): The S3 path.

        Raises:
            ValueError: If a conversion is not possible

        Returns:
            tuple[str, str]: A tuple containing bucket, key to the file.
        """
        match = self.s3_regx.match(raw_path)

        if (match is None or len(match.groups()) != 2):
            raise ValueError("Invalid MDF path: " + raw_path)

        bucket = match.group(1)
        key = match.group(2)
        return bucket, key

    def _convert_to_s3_path(self, bucket: str, key: str) -> str:
        """
        Converts a bucket and key to a full s3 path.

        Args:
            bucket (str): The bucket
            key (str): The key to the file.

        Returns:
            str: The path of the s3.
        """
        return f"s3://{bucket}/{key}"
