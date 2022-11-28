""" Abstracts operations with AWS KMS. """
import json
import boto3


class AwsSecretStore():  # pylint: disable=too-few-public-methods
    """ Contains all the operations related with AWS KMS. """

    def __init__(self, region_name: str = "eu-central-1"):
        self._secret_manager_client = boto3.client("secretsmanager", region_name=region_name)

    def get_secret(self, secret_id: str):
        """ Obtain Footage API Token. """
        get_secret_response = self._secret_manager_client.get_secret_value(SecretId=secret_id)
        return json.loads(get_secret_response["SecretString"])
