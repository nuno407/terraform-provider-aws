"""
This file should be used to create an API
to communicate between the application and
the database.
"""
import logging
import flask
from botocore.exceptions import ClientError
import boto3
from flask_cors import CORS


CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v6.0"     # Version of the current container

EXEC_TABLE_NAME = "dev-pipeline-execution" # Pipeline execution table name
ALGO_TABLE_NAME = "dev-algorithm-output"   # Algorithm output table name

REGION_NAME = "eu-central-1"

ERROR_HTTP_CODE = "500"
SUCCESS_HTTP_CODE = "200"
BAD_REQUEST_CODE = "400"

app = flask.Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


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
    # Create the necessary clients for AWS services access
    db_resource = boto3.resource('dynamodb',
                                 region_name=REGION_NAME)

    # Access specific DB table
    table = db_resource.Table(EXEC_TABLE_NAME)
    
    # TODO: if the table is big this approach should not be
    # used instead think in query() or get_item()

    try:
        # Get all info from the table
        response = table.scan(TableName=EXEC_TABLE_NAME)
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response['Items'])
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/getAllResults", methods=["GET"])
def get_all_results():
    """
    Returns status code 200 if get is successfull
    It will query a DynamoDB table to show everything in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    # Create the necessary clients for AWS services access
    db_resource = boto3.resource('dynamodb',
                                 region_name=REGION_NAME)

    # Access specific DB table
    table = db_resource.Table(ALGO_TABLE_NAME)
    
    # TODO: if the table is big this approach should not be
    # used instead think in query() or get_item()

    try:
        # Get all info from the table
        response = table.scan(TableName=ALGO_TABLE_NAME)
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response['Items'])
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/getExecItem", methods=["POST"])
def get_exec_item():
    """
    Returns status code 200 if get is successfull
    It will query a DynamoDB table to show a given item in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """

    if flask.request.method == "POST":

        if flask.request.form.get("id"):
            # Create the necessary clients for AWS services access
            db_resource = boto3.resource('dynamodb',
                                        region_name=REGION_NAME)

            # Access specific DB table
            table = db_resource.Table(ALGO_TABLE_NAME)
            
            # TODO: if the table is big this approach should not be
            # used instead think in query() or get_item()

            try:
                # Get all info from the table
                response = table.scan(TableName=ALGO_TABLE_NAME)
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response['Items'])
            except ClientError as error:
                return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code=BAD_REQUEST_CODE, message=response_msg)
    response.status_code = 400
    return response

@app.route("/getAlgoItem", methods=["GET"])
def get_algo_item():
    """
    Returns status code 200 if get is successfull
    It will query a DynamoDB table to show a given item in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """

    if flask.request.method == "POST":

        if flask.request.form.get("id"):
            # Create the necessary clients for AWS services access
            db_resource = boto3.resource('dynamodb',
                                        region_name=REGION_NAME)

            # Access specific DB table
            table = db_resource.Table(ALGO_TABLE_NAME)
            
            # TODO: if the table is big this approach should not be
            # used instead think in query() or get_item()

            try:
                # Get all info from the table
                response = table.scan(TableName=ALGO_TABLE_NAME)
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response['Items'])
            except ClientError as error:
                return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])
    
    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code=BAD_REQUEST_CODE, message=response_msg)
    response.status_code = 400
    return response


if __name__ == '__main__':

    # Define configuration for logging messages
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")

    # Start API process
    app.run("0.0.0.0", use_reloader=True, debug=True)
