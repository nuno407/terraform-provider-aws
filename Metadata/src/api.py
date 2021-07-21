"""
This file should be used to create an API
to communicate between the application and
the database.
"""
import logging
import flask
from botocore.exceptions import ClientError
import boto3

CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v5.2"      # Version of the current container
TABLE_NAME = "dev-metadata-mgmt"
REGION_NAME = "eu-central-1"
ERROR_HTTP_CODE = "500"
SUCCESS_HTTP_CODE = "200"

app = flask.Flask(__name__)

@app.route("/alive", methods=["GET"])
def alive():
    """
    Returns status code 200 if alive

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    return flask.jsonify(code=SUCCESS_HTTP_CODE, message='Ok')

@app.route("/ready", methods=["GET"])
def ready():
    """
    Returns status code 200 if ready

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    return flask.jsonify(code=SUCCESS_HTTP_CODE, message='Ready')

@app.route("/getAllData", methods=["GET"])
def get_all_data():
    """
    Returns status code 200 if get is successfull
    It will query a DynamoDB table to show everything in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    table = db_resource.Table(TABLE_NAME)
    #TODO if the table is big this approach should not be
    # used instead think in query() or get_item()
    try:
        response = table.scan()
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])
    else:
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)

if __name__ == '__main__':
    # Define configuration for logging messages
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")
    # Create the necessary clients for AWS services access
    db_resource = boto3.resource('dynamodb',
                                 region_name=REGION_NAME)
    # Start API process
    app.run("0.0.0.0", use_reloader=True, debug=True)
