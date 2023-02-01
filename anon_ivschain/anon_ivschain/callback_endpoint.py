""" Module that implements a blueprint endpoint creator """
import os
import threading

import flask
from flask import Blueprint

from basehandler.api_handler import OutputEndpointNotifier
from basehandler.entrypoint import CallbackBlueprintCreator
from basehandler.message_handler import InternalMessage
from basehandler.message_handler import OperationalMessage


class AnonymizeCallbackEndpointCreator(CallbackBlueprintCreator):  # pylint: disable=too-few-public-methods
    """
    Anonymize blueprint endpoint factory
    """
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        """
        Args:
            route_endpoint (str): endpoint route to create the blueprint e.g: /anonymize
            notifier (OutputEndpointNotifier): notifier to start the upload video thread

        Returns:
            blueprint (Blueprint): endpoint blueprint

        """
        anon_output_bp = Blueprint("anon_output_bp", __name__)

        @anon_output_bp.route(route_endpoint, methods=["POST"])
        def anonymize_output_handler() -> flask.Response:
            mandatory_parameters = [
                flask.request.files.get("file"),
                flask.request.form.get("path"),
                flask.request.form.get("uid")
            ]
            if any(param is None for param in mandatory_parameters):
                return flask.Response(status=400, response="bad request")

            requested_file = flask.request.files["file"]
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]

            path, file_format = os.path.splitext(s3_path)
            file_upload_path = path + "_anonymized" + file_format

            internal_message = OperationalMessage(
                uid=uid,
                status=InternalMessage.Status.PROCESSING_COMPLETED,
                bucket=notifier.container_services.anonymized_s3,
                input_media=s3_path,
                media_path=file_upload_path)

            thread = threading.Thread(target=notifier.upload_and_notify, kwargs={
                "chunk": requested_file.read(),
                "path": file_upload_path,
                "internal_message": internal_message})
            thread.start()

            return flask.Response(status=202, response="upload video to storage")

        return anon_output_bp
