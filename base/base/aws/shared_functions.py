""" Shared functions class for communication with AWS services. """
from datetime import datetime

from mypy_boto3_s3 import S3Client


class StsHelper:  # pylint: disable=too-few-public-methods
    """ Class to be used as a wrapper for assumed roles. Implements auto-renewall of tokens. """

    def __init__(self, sts_client, role: str, role_session: str) -> None:
        self.__role = role
        self.__role_session = role_session
        self.__client = sts_client
        self.__renew_credentials()

    def __renew_credentials(self):
        assumed_role = self.__client.assume_role(
            RoleArn=self.__role, RoleSessionName=self.__role_session)
        self.__credentials = assumed_role["Credentials"]
        self.__last_renew = datetime.now()

    def get_credentials(self) -> dict:
        """Obtain the AWS credentials. Renew of the credentials is done automatically.

        Returns:
            dict: _description_
        """
        if (datetime.now() - self.__last_renew).total_seconds() > 1800:
            self.__renew_credentials()
        return self.__credentials


class AWSServiceClients():  # pylint: disable=too-few-public-methods
    """ Class to be used as a monolith to store all AWS related clients. """

    def __init__(self, sqs_client: S3Client, s3_client: S3Client):
        self.sqs_client = sqs_client
        self.s3_client = s3_client
