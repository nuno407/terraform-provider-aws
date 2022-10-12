"""Shared functions class for communication with AWS services"""
from datetime import datetime

import boto3


class StsHelper:
    def __init__(self, sts_client, role: str, role_session: str) -> None:
        self.__role = role
        self.__role_session = role_session
        self.__client = sts_client
        self.__renew_credentials()

    def __renew_credentials(self):
        assumed_role = self.__client.assume_role(
            RoleArn=self.__role, RoleSessionName=self.__role_session)
        self.__credentials = assumed_role['Credentials']
        self.__last_renew = datetime.now()

    def get_credentials(self) -> dict:
        if (datetime.now() - self.__last_renew).total_seconds() > 1800:
            self.__renew_credentials()
        return self.__credentials


class AWSServiceClients():

    def __init__(self, sqs_client: boto3.Session.client, s3_client: boto3.Session.client):
        self.sqs_client = sqs_client
        self.s3_client = s3_client
