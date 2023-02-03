""""Flask blueprint endpoint creation module for chc"""
import os
import threading

import flask
from flask import Blueprint

from basehandler.api_handler import OutputEndpointNotifier
from basehandler.entrypoint import CallbackBlueprintCreator
from basehandler.message_handler import InternalMessage
from basehandler.message_handler import OperationalMessage


class CHCCallbackEndpointCreator(CallbackBlueprintCreator):  # pylint: disable=too-few-public-methods
    """Flask blueprint endpoint creation class for chc endpoint"""
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        """
        Args:
            route_endpoint (str): endpoint route to create the blueprint e.g: /cameracheck
            notifier (OutputEndpointNotifier): notifier to start the upload video thread

        Returns:
            blueprint (Blueprint): endpoint blueprint

        """
        chc_output_bp = Blueprint("chc_output_bp", __name__)

        @chc_output_bp.route(route_endpoint, methods=["POST"])
        def chc_output_handler() -> flask.Response:
            mandatory_parameters = [
                flask.request.files.get("metadata"),
                flask.request.form.get("path"),
                flask.request.form.get("uid")
            ]
            if any(param is None for param in mandatory_parameters):
                return flask.Response(status=400, response="bad request")

            file, uid, s3_path = flask.request.files["metadata"], flask.request.form["uid"], flask.request.form["path"]
            path, _ = os.path.splitext(s3_path)
            file_upload_path = path + "_chc.json"

            internal_message = OperationalMessage(
                uid=uid,
                status=InternalMessage.Status.PROCESSING_COMPLETED,
                bucket=notifier.container_services.anonymized_s3,
                input_media=s3_path,
                meta_path=file_upload_path)

            thread = threading.Thread(target=notifier.upload_and_notify, kwargs={
                "chunk": file.read(),
                "path": file_upload_path,
                "internal_message": internal_message})
            thread.start()

            return flask.Response(status=202, response="upload video to storage")
        return chc_output_bp
