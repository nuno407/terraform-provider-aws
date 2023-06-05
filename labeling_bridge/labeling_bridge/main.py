""" Main module. """

import os

import boto3
import flask

from base.aws.container_services import ContainerServices
from labeling_bridge.service import ApiService
from labeling_bridge.controller import init_controller

CONTAINER_NAME = "LabelingBridge"
CONTAINER_VERSION = "v1.0"  # Version of the current container
AWS_REGION = os.environ.get("AWS_REGION", default="eu-central-1")


def main():
    """Main"""
    ContainerServices.configure_logging("labeling_bridge")

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    service: ApiService = ApiService(s3_client, container_services)

    app: flask.Flask = init_controller(service)

    # Start API process
    if os.getenv("LOCAL_DEBUG"):
        app.run("127.0.0.1", port=5000, use_reloader=True)
    else:
        from waitress import serve  # pylint: disable=import-outside-toplevel
        serve(app, listen="*:5000", url_scheme="https")


if __name__ == "__main__":
    main()
