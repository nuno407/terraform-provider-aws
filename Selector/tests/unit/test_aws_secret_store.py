""" AwsSecretStore Tests. """
import json
from unittest import mock
import pytest
from selector.aws_secret_store import AwsSecretStore


@pytest.mark.unit
class TestAwsSecretStore():  # pylint: disable=too-few-public-methods
    """ Tests on AwsSecretStore Component. """

    @mock.patch("boto3.client", autospec=True)
    def test_get_secret_store(self, mock_boto_client):
        """ Tests handle_hq_queue message handler. """
        # GIVEN
        aws_secret_store = AwsSecretStore()
        mock_boto_client.return_value.get_secret_value.return_value = {"SecretString": json.dumps({"bla": "ble"})}

        # WHEN
        secret = aws_secret_store.get_secret(secret_id="my_secret")

        # THEN
        assert secret == {"bla": "ble"}
