"""Anonymize/CHC API script"""
import logging
import threading
import boto3
import flask
from baseaws.shared_functions import ContainerServices

CONTAINER_NAME = "ACAPI"    # Name of the current container
CONTAINER_VERSION = "v4.1"      # Version of the current container

app = flask.Flask(__name__)


def upload_and_send_msg(**kwargs):
    """Uploads file to specified path, sends update message
    to respective handler container

    Arguments:
        kwargs {dict} -- [dictionary containing chunk, path, api_queue and msg_body]:
            chunk {bytes} -- [binary object containing file to be uploaded to S3 bucket]
            path {string} -- [S3 path with target location for file upload]
            api_queue {string} -- [name of SQSQ of corresponding handler that needs to be updated]
            msg_body {dict} -- [dictionary containing update info to be sent in SQSQ message]
    """
    # Get data from kwargs
    chunk = kwargs.get("chunk")
    path = kwargs.get("path")
    api_queue = kwargs.get("api_queue")
    msg_body = kwargs.get("msg_body")

    # Upload file to S3 bucket
    container_services.upload_file(s3_client,
                                   chunk,
                                   container_services.anonymized_s3,
                                   path)

    # Send message to input queue of metadata container
    container_services.send_message(sqs_client,api_queue,msg_body)


@app.route("/alive", methods=["GET"])
def alive():
    """Returns status code 200 if alive

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + response message]
    """
    return flask.jsonify(code='200', message='Ok')


@app.route("/ready", methods=["GET"])
def ready():
    """Returns status code 200 if ready

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + response message]
    """
    return flask.jsonify(code='200', message='Ready')


@app.route("/anonymized", methods=["POST"])
def anonymization():
    """Checks if received request has all the required parameters:
    - if yes, uploads the received file into the anonymized S3
      bucket and sends an update message to the API-Anonymize
      SQS queue (to be received by the Anonymize container main
      script). Then, it returns status code 200 + response
      message.
    - if not, returns status code 400 + response message.

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + response message]
    """
    if flask.request.method == "POST":

        if flask.request.files.get("file") and flask.request.form.get("uid") and flask.request.form.get("path"):

            # Get info attached to request (file -> video;
            # uid -> video process id; path -> s3 path)
            file = flask.request.files["file"]
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]

            _logger.info("API status update:")

            # Rename file to be stored by adding the name of
            # the algorithm that processed the file
            path, file_format = s3_path.split('.')
            file_upload_path = path + "_anonymized." + file_format

            # Build message body
            msg_body = {}
            # Processing info
            msg_body['uid'] = uid
            msg_body['status'] = 'processing completed'
            # Output files bucket
            msg_body['bucket'] = container_services.anonymized_s3
            # Video file path
            msg_body['media_path'] = file_upload_path
            # Metadata file (json) path
            msg_body['meta_path'] = "-"

            # Define SQSQ to send message to
            api_queue = container_services.sqs_queues_list["API_Anonymize"]
            # Call thread function with request parameters
            thread = threading.Thread(target=upload_and_send_msg,
                                      kwargs={
                                          'chunk': file.read(),
                                          'path': file_upload_path,
                                          'api_queue': api_queue,
                                          'msg_body': msg_body
                                      }
                                      )

            # Start thread processing
            thread.start()
            _logger.info("-----------------------------------------------")

            response_msg = 'Accepted video storage request!'
            response = flask.jsonify(code='202', message=response_msg)
            response.status_code = 202
            return response

    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code='400', message=response_msg)
    response.status_code = 400
    return response


@app.route("/cameracheck", methods=["POST"])
def camera_check():
    """Checks if received request has all the required parameters:
    - if yes, uploads the received file into the anonymized S3
      bucket, and sends an update message to the API-CHC
      SQS queue with the results metadata (to be received by
      the CHC container main script). Then, it returns status
      code 200 + response message.
    - if not, returns status code 400 + response message.

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + response message]
    """
    if flask.request.method == "POST":

        if flask.request.files.get("metadata") and flask.request.form.get("uid") and flask.request.form.get("path"):

            # Get info attached to request (file -> video;
            # uid -> video process id; path -> s3 path)
            # metadata -> chc results json
            file = flask.request.files["metadata"]
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]

            # Upload received video to S3 bucket
            _logger.info("API status update:")

            # Rename metadata file to be stored by adding the name of
            # the algorithm that processed the file
            path, _ = s3_path.split('.')
            file_upload_path = path + "_chc.json"

            # Build message body
            msg_body = {}
            # Processing info
            msg_body['uid'] = uid
            msg_body['status'] = 'processing completed'
            # Output files bucket
            msg_body['bucket'] = container_services.anonymized_s3
            # Video file path
            msg_body['media_path'] = "-"
            # Metadata file (json) path
            msg_body['meta_path'] = file_upload_path

            # Define SQSQ to send message to
            api_queue = container_services.sqs_queues_list["API_CHC"]

            # Call thread function with request parameters
            thread = threading.Thread(target=upload_and_send_msg,
                                      kwargs={
                                          'chunk': file.read(),
                                          'path': file_upload_path,
                                          'api_queue': api_queue,
                                          'msg_body': msg_body
                                      }
                                      )

            # Start thread processing
            thread.start()

            response_msg = 'Accepted video storage request!'
            response = flask.jsonify(code='202', message=response_msg)
            response.status_code = 202
            return response

        # Return error code 400 if one or more parameters are missing
        response_msg = 'One or more request parameters missing!'
        response = flask.jsonify(code='400', message=response_msg)
        response.status_code = 400
        return response

    # Return error code 405 if wrong method
    response_msg = 'Method not allowed!'
    response = flask.jsonify(code='405', message=response_msg)
    response.status_code = 405
    return response


if __name__ == '__main__':

    # Define configuration for logging messages
    _logger = ContainerServices.configure_logging('acapi')

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3',
                             region_name='eu-central-1')
    sqs_client = boto3.client('sqs',
                              region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    # Start API process
    app.run("0.0.0.0", use_reloader=True, debug=False)
