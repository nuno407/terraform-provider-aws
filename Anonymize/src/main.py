import boto3
import json
import logging
from datetime import datetime
import pytz
from BaseAws.shared_functions import ContainerServices

###########################################################################
CONTAINER_NAME    = "Anonymize"         # Name of the current container (current possible names: SDM, Anonymize, Metadata)
CONTAINER_VERSION = "v5.0"              # Version of the current container
###########################################################################

def processing_anonymize(client, container_services, body):
    """Converts the message body to json format (for easier variable access) and executes the anonymization algorithm (WIP) for the file received and updates the relevant info in its relay list
    
    Arguments:
        client {boto3.client} -- [client used to access the S3 service]
        container_services {BaseAws.shared_functions.ContainerServices} -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from the received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received and to be sent via message to the input queues of the relevant containers]
    """  

    logging.info("Processing message..\n")  

    # Converts message body from string to dict (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # Download target file to be processed (anonymized)
    raw_file = container_services.download_file(client, container_services.raw_s3_bucket, dict_body["s3_path"])
    
    ####################################
    #
    #
    # INSERT ANONYMIZATION ALGORITHM HERE
    #
    #
    ####################################

    # Upload processed file
    container_services.upload_file(client, raw_file, container_services.anonymized_s3_bucket, dict_body["s3_path"])

    # Remove current step/container from the processing_steps list (after processing)
    if dict_body["processing_steps"][0] == CONTAINER_NAME:
        dict_body["processing_steps"].pop(0)     

    if dict_body["processing_steps"]:
        dict_body["data_status"] = "processing"     # change the current file data_status (if not already changed)
    else:
        dict_body["data_status"] = "complete"       # change the current file data_status to complete (if current step is the last one from the list)

    relay_data = dict_body                          # currently just sends the same msg that received

    logging.info("\nProcessing complete!")  
    logging.info("    -> key: {}".format(relay_data["s3_path"]))
    logging.info("    -> timestamp: {}\n".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))

    return relay_data

def main():
    """Main function

    """ 
    ###### CONFIGURATIONS ########################################################################################################

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("------ Starting Container {} (version: {}) ------\n".format(CONTAINER_NAME, CONTAINER_VERSION))
    
    # Create the necessary clients for AWS services access
    s3_client  = boto3.client('s3', region_name='eu-central-1') 
    sqs_client = boto3.client('sqs', region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)
    
    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    
    ###### MAIN LOOP ##############################################################################################################

    logging.info("\nListening to {} queue..\n\n".format(container_services.input_queue))
    
    #while(True):
    
    # Check input SQS queue for new messages
    message = container_services.listen_to_input_queue(sqs_client)
    
    if message:
    
        # Processing step
        relay_list = processing_anonymize(s3_client, container_services, message['Body'])

        # Send message to input queue of the next processing step (if applicable)
        if relay_list["processing_steps"]:
            container_services.send_message(sqs_client, container_services.output_queues_list[relay_list["processing_steps"][0]], relay_list)

        # Send message to input queue of metadata container
        container_services.send_message(sqs_client, container_services.output_queues_list["Metadata"], relay_list)

        # Delete message after processing
        container_services.delete_message(sqs_client, message['ReceiptHandle'])
    
if __name__ == '__main__':
    main()