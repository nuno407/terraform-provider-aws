"""Anonymize/CHC API script"""
import logging
import boto3
import flask
import json
from baseaws.shared_functions import ContainerServices
import subprocess
from celery import Celery

CONTAINER_NAME = "ACAPI"    # Name of the current container
CONTAINER_VERSION = "v3.0"      # Version of the current container

app = flask.Flask(__name__)
############################
broker_url = 'amqp://guest@localhost'
celery = Celery(app.name, broker=broker_url)
celery.config_from_object('celeryconfig')

@celery.task(bind=True)
def some_long_task(self, x, y):
    # Do some long task
    print(x, y)
##############################
@app.route("/alive", methods=["GET"])
def alive():
    """Returns status code 200 if alive

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + response message]
    """
    x="1"
    y="2"
    some_long_task.delay(x, y)     
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
            chunk = flask.request.files["file"]
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]

            logging.info("-----------------------------------------------")
            logging.info("API status update:")

            # Rename file to be stored by adding the name of
            # the algorithm that processed the file
            path, file_extension = s3_path.split('.')
            video_upload_path = path + "_anonymized.mp4"

            # Convert received file into bytes 
            input_video = chunk.read()
            
            # Store input video file into current working directory
            input_name = "input_video.avi"
            input_file = open(input_name, "wb")
            input_file.write(input_video)
            input_file.close()

            # Convert .avi input file into .mp4 using ffmpeg
            output_name = "output_video.mp4"
            subprocess.run(["ffmpeg", "-i", input_name, "-b:v", "27648k", output_name])

            # Load bytes from converted output file
            output_file = open(output_name, "rb")
            output_video = output_file.read()
            output_file.close()

            # Upload converted output file to S3 bucket
            container_services.upload_file(s3_client,
                                           output_video,
                                           container_services.anonymized_s3,
                                           video_upload_path)

            # Delete temporary video files
            subprocess.run(["ls", "-l"])
            subprocess.run(["rm", input_name, output_name])
            subprocess.run(["ls", "-l"])

            # Build message body
            msg_body = {}
            # Processing info
            msg_body['uid'] = uid
            msg_body['status'] = 'processing completed'
            # Output files bucket
            msg_body['bucket'] = container_services.anonymized_s3
            # Video file path
            msg_body['video_path'] = video_upload_path

            # Send message to input queue of metadata container
            api_queue = container_services.sqs_queues_list["API_Anonymize"]

            container_services.send_message(sqs_client,
                                            api_queue,
                                            msg_body)

            logging.info("-----------------------------------------------")

            response_msg = 'Stored received video on S3 bucket!'
            response = flask.jsonify(code='200', message=response_msg)
            response.status_code = 200
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
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]
            meta_body = flask.request.files["metadata"]

            # Upload received video to S3 bucket
            logging.info("-----------------------------------------------")
            logging.info("API status update:")

            # Rename metadata file to be stored by adding the name of
            # the algorithm that processed the file
            path, file_extension = s3_path.split('.')
            meta_upload_path = path + "_chc.json"

            container_services.upload_file(s3_client,
                                           meta_body,
                                           container_services.anonymized_s3,
                                           meta_upload_path)
            # Build message body
            msg_body = {}
            # Processing info
            msg_body['uid'] = uid
            msg_body['status'] = 'processing completed'
            # Output files bucket
            msg_body['bucket'] = container_services.anonymized_s3
            # Video file path
            msg_body['video_path'] = "-"
            # Metadata file (json) path
            msg_body['meta_path'] = meta_upload_path

            # Send message to input queue of metadata container
            api_queue = container_services.sqs_queues_list["API_CHC"]

            container_services.send_message(sqs_client,
                                            api_queue,
                                            msg_body)

            logging.info("-----------------------------------------------")

            response_msg = 'Stored received video on S3 bucket!'
            response = flask.jsonify(code='200', message=response_msg)
            response.status_code = 200
            return response

        # Return error code 400 if one or more parameters are missing
        response_msg = 'One or more request parameters missing!'
        response = flask.jsonify(code='400', message=response_msg)
        response.status_code = 400
        return response


if __name__ == '__main__':

    # Define configuration for logging messages
    FORMAT_LOG = "%(asctime)s: %(message)s"
    logging.basicConfig(format=FORMAT_LOG, level=logging.INFO,
                        datefmt="%H:%M:%S")

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
