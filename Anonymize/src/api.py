import boto3
import flask
import logging
from baseaws.shared_functions import ContainerServices
from werkzeug.utils import secure_filename

CONTAINER_NAME = "Anonymize"    # Name of the current container
CONTAINER_VERSION = "v5.2"      # Version of the current container

app = flask.Flask(__name__)

@app.route("/alive", methods=["GET"])
def alive():
    return flask.jsonify(code='200', message='Ok')

@app.route("/ready", methods=["GET"])
def ready():
    return flask.jsonify(code='200', message='Ready')

@app.route("/anonymized", methods=["POST"])
def chain_producer():
    if flask.request.method == "POST":

        if flask.request.files.get("file") and flask.request.form.get("uid") and flask.request.form.get("path"):

            # Get info attached to request (file -> video; uid -> video process id; path -> s3 path)
            chunk = flask.request.files["file"]
            uid = flask.request.form["uid"]
            s3_path = flask.request.form["path"]

            # Upload received video to S3 bucket
            logging.info("-----------------------------------------------")
            logging.info("API status update:")
            container_services.upload_file(s3_client,
                                           chunk,
                                           container_services.anonymized_s3,
                                           s3_path)

            # Build message body
            msg_body = {}
            msg_body['uid'] = uid
            msg_body['s3_path'] = s3_path
            msg_body['bucket'] = container_services.anonymized_s3
            msg_body['status'] = 'processing completed'

            # Send message to input queue of metadata container
            api_queue = container_services.sqs_queues_list["API_Anonymize"]
            
            container_services.send_message(sqs_client,
                                            api_queue,
                                            msg_body)
            logging.info("-----------------------------------------------")

    return flask.jsonify(code='200', message='Stored received video on S3 bucket!')

if __name__ == '__main__':

    # Define configuration for logging messages
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
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
