import os
import tempfile

from mongoengine import connect, get_connection

os.environ["FIFTYONE_DATABASE_DIR"] = tempfile.TemporaryDirectory().name
os.environ["FIFTYONE_DATABASE_ADMIN"] = "true"
os.environ["FIFTYONE_DO_NOT_TRACK"] = "true"

# Fiftyone launches a database by it self when we import it.
# Therefore we first start Fiftyone and then get the connection from it and inject
# it in our mongoengine instance connector
import fiftyone as _  # noqa
host, port = get_connection().address
connect(db="DataPrivacy", host=host, port=port, alias="DataPrivacyDB")
