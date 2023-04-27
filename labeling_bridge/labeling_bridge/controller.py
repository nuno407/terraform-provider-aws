"""
Labeling Bridge API
"""
import logging

import flask
from flask import make_response
from flask_cors import CORS
from flask_restx import Api, Resource, fields

_logger = logging.getLogger("labeling_bridge." + __name__)

ERROR_400_MSG = "Invalid or missing argument(s)"
ERROR_404_MSG = "Method not found"
ERROR_500_MSG = "Internal Server Error"


class ReverseProxied:  # pylint: disable=too-few-public-methods
    """Reverse proxy configuration class."""
    # Inspired by: http://flask.pocoo.org/snippets/35/

    def __init__(self, wsgi_app, app):
        self.wsgi_app = wsgi_app
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get("HTTP_X_SCRIPT_NAME", "/api")
        if script_name:
            if script_name.startswith("/"):
                environ["SCRIPT_NAME"] = script_name
                path_info = environ["PATH_INFO"]
                if path_info.startswith(script_name):
                    environ["PATH_INFO"] = path_info[len(script_name):]
            else:
                self.app.warning("'prefix' must start with a '/'!")

        return self.wsgi_app(environ, start_response)


def generate_exception_logs(err: Exception):
    """
    Generates exception logs to be displayed during container execution
    """
    _logger.exception(
        "The following exception occured during execution: %s", err)


def init_controller() -> flask.Flask:  # pylint: disable=too-many-locals,too-many-statements
    """Initialize Flask application defined in the controller module.

    Args:
        service (ApiService): ApiService object.
    Returns:
        flask.Flask: initialized flask app with routes registered.
    """

    # API instance initialisation
    app = flask.Flask(__name__)
    app.wsgi_app = ReverseProxied(app.wsgi_app, app)  # type: ignore
    CORS(app, resources={r"/*": {"origins": "*"}})

    # Swagger documentation initialisation
    api = Api(app, version="1.0", title="Labeling Bridge API",
              description="API used to submit labeling jobs from Voxel UI.",
              default="General endpoints", default_label="")

    # Create namespace for debug endpoints
    api.namespace("Debug endpoints", description="", path="/")

    # Error 404 general handler
    @app.errorhandler(404)
    def not_found(_):
        return make_response(flask.jsonify(message=ERROR_404_MSG, statusCode="404"), 404)

    # Common models used in most endpoints (Swagger documentation)
    error_400_model = api.model("Error_400", {  # pylint: disable=unused-variable
        "message": fields.String(example=ERROR_400_MSG),
        "statusCode": fields.String(example="400")
    })

    # Common models used in most endpoints (Swagger documentation)
    error_500_model = api.model("Error_500", {  # pylint: disable=unused-variable
        "message": fields.String(example=ERROR_500_MSG),
        "statusCode": fields.String(example="500")
    })

    # Custom model for alive code 200 response (Swagger documentation)
    alive_200_model = api.model("Alive_200", {
        "message": fields.String(example="Ok"),
        "statusCode": fields.String(example="200")
    })

    @api.route("/alive")
    class Alive(Resource):  # pylint: disable=too-few-public-methods,unused-variable
        """Class for /alive endpoint response"""
        @api.response(200, "Success", alive_200_model)
        def get(self):
            """
            Checks if API is alive
            """
            return flask.jsonify(message="Ok", statusCode="200")

    # Custom model for ready code 200 response (Swagger documentation)
    ready_200_model = api.model("Ready_200", {
        "message": fields.String(example="Ready"),
        "statusCode": fields.String(example="200")
    })

    @api.route("/ready")
    class Ready(Resource):  # pylint: disable=too-few-public-methods,unused-variable
        """Class for /ready endpoint response"""
        @api.response(200, "Success", ready_200_model)
        def get(self):
            """
            Checks if API is ready
            """
            return flask.jsonify(message="Ready", statusCode="200")

    return app
