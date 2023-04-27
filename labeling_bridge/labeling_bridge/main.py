""" Main module. """

import os

import boto3
import flask

from base.aws.container_services import ContainerServices
from labeling_bridge.controller import init_controller

CONTAINER_NAME = "LabelingBridge"
CONTAINER_VERSION = "v1.0"  # Version of the current container
AWS_REGION = os.environ.get("AWS_REGION", default="eu-central-1")

if __name__ == "__main__":
    ContainerServices.configure_logging("labeling_bridge")

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    app: flask.Flask = init_controller()

    # Start API process
    if os.getenv("LOCAL_DEBUG"):
        app.run("127.0.0.1", port=7777, use_reloader=True)
    else:
        from waitress import serve
        serve(app, listen="*:5000", url_scheme="https")
