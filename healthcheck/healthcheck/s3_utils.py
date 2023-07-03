""" S3 utils """
from kink import inject

from base.aws.s3 import S3Controller
from base.model.artifacts import Artifact
from healthcheck.exceptions import AnonymizedFileNotPresent, RawFileNotPresent
from healthcheck.model import S3Params


@inject
class S3Utils:
    """ S3 interface utility class. """

    def __init__(self,
                 s3_params: S3Params,
                 s3_controller: S3Controller):
        self.__s3_params = s3_params
        self.__s3_controller = s3_controller

    @classmethod
    def full_s3_path(cls, tenant_name: str, file_key: str) -> str:
        """
        Given an s3 file name, appends the root folder to the key.

        Args:
            file_key (str): name of the file.

        Returns:
            str: The S3 key fo the file requested
        """
        return f"{tenant_name}/{file_key}"

    def is_s3_anonymized_file_present_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """
        Check for the presence of file in anonymize S3.
        If it doesn't exist, raises an exception.

        Args:
            file_name (str): The file name to be searched (This is not the full key)
            artifact (Artifact): artifact message
        Raises:
            AnonymizedFileNotPresent: If file is not present in the anonymize bucket.
        """
        bucket = self.__s3_params.s3_bucket_anon
        path = S3Utils.full_s3_path(artifact.tenant_id, file_name)
        if not self.__s3_controller.check_s3_file_exists(bucket, path):
            raise AnonymizedFileNotPresent(artifact.artifact_id, f"Anonymized file {file_name} not found")

    def is_s3_raw_file_present_or_raise(self, file_name: str, artifact: Artifact) -> None:
        """
        Check for the presence of file in raw S3.
        If it doesn't exist, raises an exception.

        Args:
            file_name (str): The file name to be searched (This is not the full key)

        Raises:
            RawFileNotPresent: If file is not present in the anonymize bucket.
        """
        bucket = self.__s3_params.s3_bucket_raw
        path = S3Utils.full_s3_path(artifact.tenant_id, file_name)
        if not self.__s3_controller.check_s3_file_exists(bucket, path):
            raise RawFileNotPresent(artifact.artifact_id, f"Raw file {file_name} not found")
