"""Anonymize container script"""
import json
import logging
import uuid
import boto3
from baseaws.shared_functions import ContainerServices
import requests
import subprocess

CONTAINER_NAME = "Anonymize"    # Name of the current container
CONTAINER_VERSION = "v8.0"      # Version of the current container


def request_processing(client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    and sends an API request for the ivs feature chain container with the
    file downloaded to be processed.

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
    """
    logging.info("Processing pipeline message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Download target file to be processed (anonymized)
    raw_file = container_services.download_file(client,
                                                container_services.raw_s3,
                                                dict_body["s3_path"])

    # Create a random uuid to identify a given video anonymization process
    uid = str(uuid.uuid4())

    # Add entry for current video relay list on pending queue
    container_services.update_pending_queue(client, uid, "insert", dict_body)

    # Prepare data to be sent on API request
    payload = {'uid': uid,
               'path': dict_body["s3_path"],
               'mode': 'anonymize'}
    files = [('video', raw_file)]

    # Define settings for API request
    ip_pod = container_services.ivs_api["address"]
    port_pod = container_services.ivs_api["port"]
    req_command = container_services.ivs_api["endpoint"]

    # Build address for request
    addr = 'http://{}:{}/{}'.format(ip_pod, port_pod, req_command)

    # Send API request (POST)
    try:
        response = requests.post(addr, files=files, data=payload)
        logging.info("API POST request sent! (uid: %s)", uid)
        logging.info("IVS Chain response: %s", response.text)
    except requests.exceptions.ConnectionError:
        logging.info("\n######################## Exception #########################")
        logging.exception("The following exception occured during execution:")
        logging.info("############################################################\n")

    # TODO: ADD EXCEPTION HANDLING IF API NOT AVAILABLE (except Exception as e:)

def update_processing(client, container_services, body):
    """Converts the message body to json format (for easier variable access)
    and executes the anonymization algorithm (WIP) for the file received and
    updates the relevant info in its relay list

    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the
                          received message]
    Returns:
        relay_data {dict} -- [dict with the updated info after file processing
                              and to be sent via message to the input queues of
                              the relevant containers]
        output_info {dict} -- [dict with the output S3 path and bucket
                               information, where the CHC video will be
                               stored]
    """
    logging.info("Processing API message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    msg_body = json.loads(new_body)

    # Retrives relay_list based on uid received from api message
    relay_data = container_services.update_pending_queue(client,
                                                         msg_body['uid'],
                                                         "read")

    ######################################################################
    # VIDEO CONVERSION (.avi -> .mp4)

    logging.info("Starting conversion (AVI to MP4) process..\n")

    # Defining videos/logs paths
    avi_path = msg_body['video_path']
    path = avi_path.split('.')[0]
    mp4_path = path + ".mp4"
    logs_path = path.split("_Anonymize")[0] + "_conversion_logs.txt"

    # Defining temporary files names
    input_name = "input_video.avi"
    output_name = "output_video.mp4"
    logs_name = "logs.txt"

    # Download target file to be converted
    avi_video = container_services.download_file(client,
                                                container_services.anonymized_s3,
                                                avi_path)

    # Store input video file into current working directory
    with open(input_name, "wb") as input_file:
        input_file.write(avi_video)

    with open(logs_name, 'w') as logs_write:
        # Convert .avi input file into .mp4 using ffmpeg
        conv_logs = subprocess.Popen(["ffmpeg", "-i", input_name, "-b:v",
                                      "27648k", output_name],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      universal_newlines=True)
        
        # Save conversion logs into txt file
        for line in conv_logs.stdout:
            logs_write.write(line)

    # Load bytes from converted output file
    with open(output_name, "rb") as output_file:
        output_video = output_file.read()

    logging.info("\nConversion complete!\n")

    # Upload converted output file to S3 bucket
    container_services.upload_file(client,
                                   output_video,
                                   container_services.anonymized_s3,
                                   mp4_path)

    # Load bytes from logs file
    with open(logs_name, "rb") as logs_bytes:
        logs_file = logs_bytes.read()
    
    # Upload conversion logs to S3 bucket
    container_services.upload_file(client,
                                   logs_file,
                                   container_services.anonymized_s3,
                                   logs_path)

    # Delete temporary video files
    subprocess.run(["rm", input_name, output_name, logs_name])

    #########################################################################################

    # Retrieve output info from received message
    output_info = {}
    output_info['bucket'] = msg_body['bucket']
    output_info['video_path'] = mp4_path
    output_info['meta_path'] = msg_body['meta_path']

    # Remove current step/container from the processing_steps
    # list (after processing)
    if relay_data["processing_steps"][0] == CONTAINER_NAME:
        relay_data["processing_steps"].pop(0)

    if relay_data["processing_steps"]:
        # change the current file data_status (if not already changed)
        relay_data["data_status"] = "processing"
    else:
        # change the current file data_status to complete
        # (if current step is the last one from the list)
        relay_data["data_status"] = "complete"

    # Remove uid entry from pending queue
    container_services.update_pending_queue(client,
                                            msg_body['uid'],
                                            "delete")

    container_services.display_processed_msg(relay_data["s3_path"],
                                             msg_body['uid'])

    return relay_data, output_info


def main():
    """Main function"""

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
                                                   CONTAINER_VERSION)

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

    # Define additional input SQS queues to listen to
    # (container_services.input_queue is the default queue
    # and doesn't need to be declared here)
    api_sqs_queue = container_services.sqs_queues_list['API_Anonymize']

    logging.info("\nListening to input queue(s)..\n")

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Processing request
            request_processing(s3_client,
                               container_services,
                               message['Body'])

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])

        # Check API SQS queue for new update messages
        message_api = container_services.listen_to_input_queue(sqs_client,
                                                               api_sqs_queue)

        if message_api:
            # Processing update
            relay_list, out_s3 = update_processing(s3_client,
                                                   container_services,
                                                   message_api['Body'])

            # Send message to input queue of the next processing step
            # (if applicable)
            if relay_list["processing_steps"]:
                next_step = relay_list["processing_steps"][0]
                next_queue = container_services.sqs_queues_list[next_step]
                container_services.send_message(sqs_client,
                                                next_queue,
                                                relay_list)

            # Add the algorithm output flag/info to the relay_list sent
            # to the metadata container so that an item for this processing
            # run can be created on the Algo Output DB
            relay_list['output'] = out_s3

            # Send message to input queue of metadata container
            metadata_queue = container_services.sqs_queues_list["Metadata"]
            container_services.send_message(sqs_client,
                                            metadata_queue,
                                            relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message_api['ReceiptHandle'],
                                              api_sqs_queue)


if __name__ == '__main__':
    main()
