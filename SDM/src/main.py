import boto3
import json
import re
from datetime import datetime
import pytz
import logging

###########################################################################
CONTAINER_NAME    = "SDM"               # Name of the current container (current possible names: SDM, Anonymize, Metadata)
CONTAINER_VERSION = "v4.1"              # Version of the current container
###########################################################################

def load_config_vars():
    """Gets configuration json file from s3 bucket and initialises the global variables based on the info from that file

    """
    # Create S3 client
    s3_client = boto3.client('s3')

    # Bucket and path for the config file to be used
    s3_config_bucket = 'dev-rcd-raw-video-files'
    s3_config_file = 'uber/config_file_containers.json'

    logging.info("Loading parameters from config file (path: {})..".format(s3_config_bucket+'/'+s3_config_file))

    # Send request to access the config file (json)
    response = s3_client.get_object(
        Bucket=s3_config_bucket,
        Key=s3_config_file
    )
    
    # Load config file (botocore.response.StreamingBody) content to dictionary
    dict_body = json.loads(response['Body'].read().decode("utf-8"))

    # List of the queues attached to each container
    global OUTPUT_QUEUES_LIST
    OUTPUT_QUEUES_LIST = dict_body['OUTPUT_QUEUES_LIST']

    # Name of the input queue attached to the current container
    global INPUT_QUEUE
    INPUT_QUEUE = OUTPUT_QUEUES_LIST[CONTAINER_NAME]

    # States if current container has DB connection (true) or not (false)
    global DB_CONNECTION_ENABLED 
    DB_CONNECTION_ENABLED = dict_body['DB_CONNECTION_ENABLED'][CONTAINER_NAME]

    # Name of the dynamoDB table used to store metadata 
    global DB_TABLE_NAME
    DB_TABLE_NAME = dict_body['DB_TABLE_NAME']

    # List of processing steps required for each file based on the MSP
    global SDM_PROCESSING_LIST
    SDM_PROCESSING_LIST = dict_body['SDM_PROCESSING_LIST']

    logging.info("Load complete!\n")

def listen_to_input_queue():
    """Logs into the respective input SQS queue for the current container and checks for new messages.

    - If the queue is empty, it waits up until 20s for new messages before restarting the loop. 

    - If there is a new message, the following steps are executed:
        > the message body is processed and a relay list is created based on the info collected
        > a message containing the relay list is sent to each output SQS queue of the current container
        > if the variable "DB_CONNECTION_ENABLED" is set to true, a request for item creation/update for the file mentioned in the received message is also sent to the respective database
    """

    # Create SQS client
    sqs = boto3.client('sqs', region_name='eu-central-1')

    response_input = sqs.get_queue_url(QueueName=INPUT_QUEUE)
    input_queue_url = response_input['QueueUrl']   

    logging.info('Listening to %s queue..\n', INPUT_QUEUE)

    # Main loop
    while(True):

        # Receive messages
        response = sqs.receive_message(
            QueueUrl=input_queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=20
        )

        # If queue has new messages
        if 'Messages' in response:
            # Select the first message received (by default, it only receives 1 message per enquiry - set above by MaxNumberOfMessages parameter)
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            
            # Process message body
            relay_list = processing_pipeline(message['Body'])
            logging.info("Message received!\n")
            logging.info("    -> id:  {}".format(message['MessageId']))
            logging.info("    -> key: {}".format(relay_list["s3_path"]))
            logging.info("    -> timestamp: {}\n".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))
            logging.info("Processing message..")  

            if DB_CONNECTION_ENABLED:
                
                # Insert data to db
                connect_to_db(relay_list, message['MessageAttributes'])

                # Send message to output queue
                response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST["Output"])
                output_queue_url = response_output['QueueUrl'] 
                send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST["Output"])
                logging.info("[{}]  Message sent to {} queue".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), OUTPUT_QUEUES_LIST["Output"]))

            else:

                # Send message to output queue (if there are steps left)
                if relay_list["processing_steps"]:
                    response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]])
                    output_queue_url = response_output['QueueUrl']   
                    send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]])
                    logging.info("[{}]  Message sent to {} queue".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]]))

                # Send message to metadata mgmt queue
                response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST["Metadata"])
                output_queue_url = response_output['QueueUrl'] 
                send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST["Metadata"])
                logging.info("[{}]  Message sent to {} queue".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), OUTPUT_QUEUES_LIST["Metadata"]))

            # Delete received message
            sqs.delete_message(
                QueueUrl=input_queue_url,
                ReceiptHandle=receipt_handle
            )

            logging.info("\nListening to {} queue..\n".format(INPUT_QUEUE))

def processing_pipeline(body):
    """Converts the message body to json format (for easier variable access) and calls the appropriate processing function based on the name of the current container
    
    Arguments:
        body {str} -- [string containing the body info from the received message]
    Returns:
        relay_data {dict} -- [dict with the relevant info for the file received and to be sent via message to the output queues and/or via creation/update item request to the database]
    """
    # Converts message body from string to dict (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # PROCESSING PIPELINE
    if CONTAINER_NAME == "SDM":       
        # calls processing function for container SDM
        relay_data = processing_sdm(dict_body)

    elif CONTAINER_NAME == "Anonymize":   
        # calls processing function for container Anonymize
        relay_data = processing_anonymize(dict_body)

    elif CONTAINER_NAME == "Metadata":  
        # calls processing function for container Metadata
        relay_data = processing_metadata(dict_body)

    return relay_data

def processing_sdm(dict_body):
    """Retrieves the MSP name from the message received and creates a relay list for the current file 
    
    Arguments:
        dict_body {dict} -- [dict containing the body info from the received message]
    Returns:
        relay_data {dict} -- [dict with the relevant info for the file received and to be sent via message to the input queues of the relevant containers]
    """
    # Access key value from msg body
    key_value = dict_body["Records"][0]["s3"]["object"]["key"]
    msp = key_value.split('/')[0]

    # Creates relay list to be used by other containers
    relay_data = {}
    relay_data["processing_steps"] = SDM_PROCESSING_LIST[msp]
    relay_data["s3_path"] = key_value
    relay_data["data_status"] = "received"

    return relay_data

def processing_anonymize(dict_body):
    """Executes the anonymization algorithm (WIP) for the file received and updates the relevant info in its relay list
    
    Arguments:
        dict_body {dict} -- [dict containing the body info from the received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received and to be sent via message to the input queues of the relevant containers]
    """  
    ####################################
    #
    #
    # INSERT ANONYMIZATION ALGORITHM HERE + store_file()
    #
    #
    ####################################

    # remove current step/container from the processing_steps list (after processing)
    if dict_body["processing_steps"][0] == CONTAINER_NAME:
        dict_body["processing_steps"].pop(0)     

    if dict_body["processing_steps"]:
        dict_body["data_status"] = "processing"     # change the current file data_status (if not already changed)
    else:
        dict_body["data_status"] = "complete"       # change the current file data_status to complete (if current step is the last one from the list)

    relay_data = dict_body                          # currently just sends the same msg that received

    return relay_data

def processing_metadata(dict_body):
    """Copies the relay list info received from other containers 
    
    Arguments:
        dict_body {dict} -- [dict containing the body info from the received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received and to be sent via message to the output queue]
    """  
    relay_data = dict_body                          # currently just sends the same msg that received

    return relay_data

def send_message(sqs_client, output_queue_url, data, output_queue_name):
    """Prepares the message attributes + body and sends a message to the target queue with the prepared info
    
    Arguments:
        sqs_client {boto3.client} -- [client used to access the SQS service]
        output_queue_url {string} -- [URL of the destination output SQS queue]
        data {dict} -- [dict containing the info to be sent in the message body]
        output_queue_name {string} -- [Name of the destination output SQS queue]
    """
    # Add attributes to message
    msg_attributes = {
                        'SourceContainer': {
                            'DataType': 'String',
                            'StringValue': CONTAINER_NAME
                        },
                        'FromQueue': {
                            'DataType': 'String',
                            'StringValue': INPUT_QUEUE
                        },
                        'ToQueue': {
                            'DataType': 'String',
                            'StringValue': output_queue_name
                        }
                    }

    # Send message to SQS queue
    response = sqs_client.send_message(
        QueueUrl=output_queue_url,
        DelaySeconds=1,
        MessageAttributes= msg_attributes,
        MessageBody=str(data)
    )

def connect_to_db(data, attributes):
    """Connects to the DynamoDB table and checks if an item with an id equal to the file name already exists:
    
    - If yes, updates some of the item parameters with new values provided as inputs (data and attributes)
    - If not, creates a new item with the values provided in the data and attributes inputs
    
    Arguments:
        data {dict} -- [dict containing the info to be sent in the message body]
        attributes {dict} -- [dict containing the received message attributes (to check its contents, please refer to the msg_attributes dict structure created in the send_message function)]
    """
    # Connect to DB resource
    dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
    
    # Select table to use
    table = dynamodb.Table(DB_TABLE_NAME)
    
    # Get filename (id) from message received
    get_file_name = re.search('/(.*)\.', data["s3_path"])
    unique_id = get_file_name.group(1)

    # Check if item with that name already exists
    response = table.get_item(Key={'id': unique_id})
    if 'Item' in response:
        # Update already existing item
        table.update_item(
                            Key={ 'id': unique_id },
                            UpdateExpression='SET data_status = :val1, info_source = :val2, last_updated = :val3',
                            ExpressionAttributeValues={ 
                                                        ':val1': data['data_status'],
                                                        ':val2': attributes['SourceContainer']['StringValue'],
                                                        ':val3': str(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                                                    },
                            ReturnValues="UPDATED_NEW"
                        )
        logging.info("[{}]  DB item (Id: {}) updated!".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), unique_id))
    else:
        # Insert item if not created yet
        item_db = {
                    'id': unique_id,
                    'from_container': CONTAINER_NAME,
                    's3_path': data['s3_path'],
                    'data_status':  data['data_status'],
                    'info_source': attributes['SourceContainer']['StringValue'],
                    'processing_list': data['processing_steps'], 
                    'last_updated': str(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                }
        table.put_item(Item=item_db)
        logging.info("[{}]  DB item (Id: {}) created!".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), unique_id))

def store_file(path_file, s3_bucket, target_name):
    """(WIP - DO NOT USE IT YET) Stores a given file in the selected s3 bucket
    
    Arguments:
        path_file {string} -- [string containing the current path for the file to be stored]
        s3_bucket {string} -- [name of the destination s3 bucket]
        target_name {string} -- [string containg the path + file name to be used for the file in the destination s3 bucket (e.g. 'uber/test_file_s3.txt')]
    """
    logging.info("store_file function -> WORK IN PROGRESS")
    
    # Create S3 client
    s3_client = boto3.client('s3')

    # Hardcodded parameters (to be changed)
    path_file = r'C:\Users\PEM3BRG\Desktop\test_file_s3.txt'    
    s3_bucket = 'dev-rcd-anonymized-video-files'
    target_name = 'uber/test_file_s3.txt'

    # Open file and send put object request 
    with open(path_file, 'rb') as file_object:

        response = s3_client.put_object(
                                        Body=file_object,
                                        Bucket=s3_bucket,
                                        Key= target_name,
                                        ServerSideEncryption='aws:kms'
                                        )

    logging.info(response)
    
def main():
    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("------ Starting Container {} (version: {}) ------\n".format(CONTAINER_NAME, CONTAINER_VERSION))
    
    # Load global variable values from config json file (S3 bucket)
    load_config_vars()
    # Start listening to SQS queue
    listen_to_input_queue()

if __name__ == '__main__':
    main()