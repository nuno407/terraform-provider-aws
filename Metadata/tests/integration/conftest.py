"""conftest contains common fixtures and mocks for all unit tests"""
import os
import tempfile
from mongoengine import connect, get_connection

from base.testing.mock_functions import set_mock_aws_credentials

set_mock_aws_credentials()
os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"

# Fiftyone launches a database by it self when we import it.
# Therefore we first start Fiftyone and then get the connection from it and inject
# it in our mongoengine instance connector
import fiftyone as _  # noqa
host, port = get_connection().address
connect(db="DataIngestion", host=host, port=port, alias="DataIngestionDB")
