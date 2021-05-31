import boto3
import json
import re
from datetime import datetime
import pytz

###########################################################################
CONTAINER_NAME = "Metadata"  # Name of the current container (current possible names: SDM, Anonymize, Metadata)

OUTPUT_QUEUES_LIST = {
                        "SDM":          "dev-terraform-queue-s3-sdm",
                        "Anonymize":    "dev-terraform-queue-anonymize",
                        "PreProcess":   "dev-terraform-queue-preprocessing",
                        "Metadata":     "dev-terraform-queue-metadata",
                        "Output":       "dev-terraform-queue-output"
                   }

INPUT_QUEUE = OUTPUT_QUEUES_LIST[CONTAINER_NAME]  # "dev-terraform-queue-s3-sdm"

DB_CONNECTION_ENABLED   = True                 # states if container has connection to DB (DB_CONNECTION_ENABLED = True) or not (DB_CONNECTION_ENABLED = False). Only true for metadata container
DB_TABLE_NAME           = "dev-metadata-mgmt"   # if DB_CONNECTION_ENABLED = True, then this variable needs to have a valid DB table name (otherwise just leave it as "")

SDM_PROCESSING_LIST = {
                        "uber": ["Anonymize"],
                        "lyft": ["Anonymize", "PreProcess"],
                        "lync": ["Anonymize"]    
                    }
# ["Anonymize", "PreProcess", "Labelling"]
TIMEZONE = pytz.timezone('Europe/London')
###########################################################################

def listen_to_input_queue():

    # Create client
    sqs = boto3.client('sqs', region_name='eu-central-1')

    response_input = sqs.get_queue_url(QueueName=INPUT_QUEUE)
    input_queue_url = response_input['QueueUrl']   

    print("Listening to {} queue..\n".format(INPUT_QUEUE))

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

        if 'Messages' in response:

            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            
            # Process message body
            relay_list = processing_pipeline(message['Body'])
            print("Message received!\n")
            print("    -> id:  {}".format(message['MessageId']))
            print("    -> key: {}\n".format(relay_list["s3_path"]))
            print("    -> timestamp: {}\n".format(datetime.now(TIMEZONE)))
            print("Processing message..")    

            if DB_CONNECTION_ENABLED:
                
                # Insert data to db
                connect_to_db(relay_list, message['MessageAttributes'])

                # Send message to output queue
                response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST["Output"])
                output_queue_url = response_output['QueueUrl'] 
                send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST["Output"])
                print("Message sent to {} queue ({})".format(OUTPUT_QUEUES_LIST["Output"], datetime.now(TIMEZONE)))

            else:

                # Send message to output queue (if there are steps left)
                if relay_list["processing_steps"]:
                    response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]])
                    output_queue_url = response_output['QueueUrl']   
                    send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]])
                    print("Message sent to {} queue ({})".format(OUTPUT_QUEUES_LIST[relay_list["processing_steps"][0]], datetime.now(TIMEZONE)))

                # Send message to metadata mgmt queue
                response_output = sqs.get_queue_url(QueueName=OUTPUT_QUEUES_LIST["Metadata"])
                output_queue_url = response_output['QueueUrl'] 
                send_message(sqs, output_queue_url, relay_list, OUTPUT_QUEUES_LIST["Metadata"])
                print("Message sent to {} queue ({})".format(OUTPUT_QUEUES_LIST["Metadata"], datetime.now(TIMEZONE)))


            # Delete received message
            sqs.delete_message(
                QueueUrl=input_queue_url,
                ReceiptHandle=receipt_handle
            )
            print()
            print("Listening to {} queue..\n".format(INPUT_QUEUE))
            
def processing_pipeline(body):

    # PROCESSING STEPS FOR SDM CONTAINER
    if CONTAINER_NAME == "SDM":       
        # Converts message body from string to dict (in order to perform index access)
        dict_body = json.loads(body)
        # Access key value from msg body
        key_value = dict_body["Records"][0]["s3"]["object"]["key"]
        msp = key_value.split('/')[0]

        relay_data = {}
        relay_data["processing_steps"] = SDM_PROCESSING_LIST[msp]
        relay_data["s3_path"] = key_value
        relay_data["data_status"] = "received"
        
    # PROCESSING STEPS FOR ANONYMIZE CONTAINER
    elif CONTAINER_NAME == "Anonymize":   
        # Converts message body from string to dict (in order to perform index access)
        new_body = body.replace("\'", "\"")
        dict_body = json.loads(new_body)
        
        #
        #
        # INSERT ANONYMIZATION ALGORITHM HERE + store_file()
        #
        #
        #

        # remove current step/container from the processing_steps list (after processing)
        if dict_body["processing_steps"][0] == CONTAINER_NAME:
            dict_body["processing_steps"].pop(0)     

        if dict_body["processing_steps"]:
            dict_body["data_status"] = "processing"      # change the current file data_status (if not already changed)
        else:
            dict_body["data_status"] = "complete"
        relay_data = dict_body                        # currently just sends the same msg that received

    # PROCESSING STEPS FOR METADATA CONTAINER
    elif CONTAINER_NAME == "Metadata":  
        # Converts message body from string to dict (in order to perform index access)
        new_body = body.replace("\'", "\"")
        #print(new_body)
        relay_data = json.loads(new_body) 

    return relay_data

def send_message(sqs_client, output_queue_url, data, output_queue_name):

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

    #print(response['MessageId'])

def connect_to_db(data, attributes):
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
                            UpdateExpression='SET data_status = :val1, info_source = :val2 ',
                            ExpressionAttributeValues={ 
                                                        ':val1': data['data_status'],
                                                        ':val2': attributes['SourceContainer']['StringValue']
                                                    },
                            ReturnValues="UPDATED_NEW"
                        )
        print("DB item (Id: {}) updated ({})!".format(unique_id, datetime.now(TIMEZONE)))
    else:
        # Insert item if not created yet
        item_db = {
                    'id': unique_id,
                    'from_container': CONTAINER_NAME,
                    's3_path': data['s3_path'],
                    'data_status':  data['data_status'],
                    'info_source': attributes['SourceContainer']['StringValue'],
                    'processing_list': data['processing_steps']
                }
        table.put_item(Item=item_db)
        print("DB item (Id: {}) created ({})!".format(unique_id, datetime.now(TIMEZONE)))

def store_file():
    # WORK IN PROGRESS
    print("store_file function -> WORK IN PROGRESS")

    s3_client = boto3.client('s3')

    file_path = r'C:\Users\PEM3BRG\Desktop\test_file_s3.txt'
    with open(file_path, 'rb') as file_object:

        response = s3_client.put_object(
                                        Body=file_object,
                                        Bucket='dev-rcd-anonymized-video-files',
                                        Key='uber/test_file_s3.txt',
                                        ServerSideEncryption='aws:kms'
                                        )

    print(response)
    
def main():
    print("------ Starting Container {} ------".format(CONTAINER_NAME))
    # Start listening to SQS queue
    listen_to_input_queue()

if __name__ == '__main__':
    main()