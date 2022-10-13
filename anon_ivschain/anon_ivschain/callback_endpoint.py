import os
import threading

import flask
from flask import Blueprint

from basehandler.api_handler import OutputEndpointNotifier
from basehandler.entrypoint import CallbackBlueprintCreator


class AnonymizeCallbackEndpointCreator(CallbackBlueprintCreator):
    @staticmethod
    def create(route_endpoint: str, notifier: OutputEndpointNotifier) -> Blueprint:
        '''
        Args:
            route_endpoint (str): endpoint route to create the blueprint e.g: /anonymize
            notifier (OutputEndpointNotifier): notifier to start the upload video thread

        Returns:
            blueprint (Blueprint): endpoint blueprint

        '''
        anon_output_bp = Blueprint('anon_output_bp', __name__)

        @anon_output_bp.route(route_endpoint, methods=["POST"])
        def anonymize_output_handler() -> flask.Response:
            mandatory_parameters = [
                flask.request.files.get("file"),
                flask.request.form.get("path"),
                flask.request.form.get("uid")
            ]
            if any([param is None for param in mandatory_parameters]):
                return flask.Response(status=400, response='bad request')

            file, uid, s3_path = flask.request.files["file"], flask.request.form["uid"], flask.request.form["path"]
            path, file_format = os.path.splitext(s3_path)
            file_upload_path = path + "_anonymized" + file_format

            msg_body = {}
            msg_body['uid'] = uid
            msg_body['status'] = 'processing completed'
            msg_body['bucket'] = notifier.container_services.anonymized_s3
            msg_body['input_media'] = s3_path
            msg_body['media_path'] = file_upload_path
            msg_body['meta_path'] = "-"

            thread = threading.Thread(target=notifier.upload_and_notify, kwargs={
                'chunk': file.read(),
                'path': file_upload_path,
                'msg_body': msg_body})
            thread.start()

            return flask.Response(status=202, response='upload video to storage')

        return anon_output_bp
