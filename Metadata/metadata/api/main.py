"""
Metadata API
"""
import os

import boto3
import flask

from base.aws.container_services import ContainerServices
from metadata.api.controller import init_controller
from metadata.api.db import Persistence
from metadata.api.service import ApiService
from metadata.common.constants import AWS_REGION

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v8.0"

if __name__ == "__main__":
    # Define configuration for logging messages
    ContainerServices.configure_logging("metadata_api")
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    container_services.load_config_vars(s3_client)
    db_connstring = container_services.get_db_connstring()
    db_tables = container_services.db_tables
    persistence = Persistence(db_connstring, db_tables)
    service = ApiService(persistence, s3_client)

    app: flask.Flask = init_controller(service)

    # Start API process
    if os.getenv("LOCAL_DEBUG"):
        app.run(
            "127.0.0.1", port=7777, use_reloader=True)
    else:
        from waitress import serve
        serve(app, listen="*:5000", url_scheme="https")
