import logging
import boto3
import json
import re
from datetime import datetime
import pytz

class ContainerServices(object):
    """ContainerServices

    Class container comprised of all the necessary tools to establish connection and interact with the various AWS services
    """

    def __init__(self, container, version): 
        # Config variables
        self.__output_queues_list    = {}
        self.__input_queue           = ""
        self.__db_connection_enabled = False
        self.__db_table_name         = ""
        self.__sdm_processing_list   = {}
        self.__raw_s3_bucket         = ""
        self.__anonymized_s3_bucket  = ""
        # Container info
        self.__container_name        = container
        self.__container_version     = version
        # Bucket and path for the config file to be used
        self.__s3_config_bucket      = 'dev-rcd-config-files'
        self.__s3_config_file        = 'containers/config_file_containers.json'


    @property
    def output_queues_list(self):
        return self.__output_queues_list

    @property
    def input_queue(self):
        return self.__input_queue

    @property
    def db_connection_enabled(self):
        return self.__db_connection_enabled

    @property
    def db_table_name(self):
        return self.__db_table_name

    @property
    def sdm_processing_list(self):
        return self.__sdm_processing_list

    @property
    def raw_s3_bucket(self):
        return self.__raw_s3_bucket

    @property
    def anonymized_s3_bucket(self):
        return self.__anonymized_s3_bucket

    def load_config_vars(self, client):
        """Gets configuration json file from s3 bucket and initialises the respective class variables based on the info from that file

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
        """

        logging.info("Loading parameters from config file (path: {})..".format(self.__s3_config_bucket+'/'+self.__s3_config_file))

        # Send request to access the config file (json)
        response = client.get_object(
            Bucket=self.__s3_config_bucket,
            Key=self.__s3_config_file
        )
        
        # Load config file (botocore.response.StreamingBody) content to dictionary
        dict_body = json.loads(response['Body'].read().decode("utf-8"))

        # List of the queues attached to each container
        self.__output_queues_list = dict_body['OUTPUT_QUEUES_LIST']

        # Name of the input queue attached to the current container
        self.__input_queue = self.__output_queues_list[self.__container_name]

        # States if current container has DB connection (true) or not (false) 
        self.__db_connection_enabled = dict_body['DB_CONNECTION_ENABLED'][self.__container_name]

        # Name of the dynamoDB table used to store metadata 
        self.__db_table_name = dict_body['DB_TABLE_NAME']

        # List of processing steps required for each file based on the MSP
        self.__sdm_processing_list = dict_body['SDM_PROCESSING_LIST']

        # Name of the S3 bucket used to store raw video files
        self.__raw_s3_bucket = dict_body['RAW_S3_BUCKET']

        # Name of the S3 bucket used to store anonymized video files
        self.__anonymized_s3_bucket = dict_body['ANONYMIZED_S3_BUCKET']

        logging.info("Load complete!\n")

    def get_sqs_queue_url(self, client, queue_name):
        """Retrieves the URL for a given SQS queue

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            queue_name {string} -- [Name of the SQS queue]
        Returns:
            queue_url {string} -- [URL of the SQS queue]
        """   
        # Request for queue url
        response = client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']  
        
        return queue_url

    def listen_to_input_queue(self, client):
        """Logs into the input SQS queue of a given container and checks for new messages.

        - If the queue is empty, it waits up until 20s for new messages before continuing

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
        Returns:
            message {dict} -- [dict with the received message content (for more info please check the response syntax of the Boto3 SQS.client.receive_message method). If no message is received, returns None]
        """

        # Get URL for container input SQS queue
        input_queue_url = self.get_sqs_queue_url(client, self.__input_queue) 

        # Receive message(s)
        response = client.receive_message(
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

            logging.info("-----------------------------------------------")
            logging.info("Message received!")
            logging.info("    -> id:  {}".format(message['MessageId']))
            logging.info("    -> timestamp: {}\n".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))

            return message
        else:
            return None
    
    def delete_message(self, client, receipt_handle):
        """Deletes received SQS message
        
        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            receipt_handle {string} -- [Receipt that identifies the received message to be deleted]
        """
        # Get URL for container input SQS queue 
        input_queue_url = self.get_sqs_queue_url(client, self.__input_queue) 

        # Delete received message
        client.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle
        )

        logging.info("-----------------------------------------------")
        logging.info("\n\nListening to {} queue..\n\n".format(self.__input_queue))

    def send_message(self, client, destination_queue_name, data):
        """Prepares the message attributes + body and sends a message with that information to the target queue
        
        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            destination_queue_name {string} -- [Name of the destination output SQS queue]
            data {dict} -- [dict containing the info to be sent in the message body]
        """

        # Get URL for target output SQS queue
        destination_queue_url = self.get_sqs_queue_url(client, destination_queue_name) 

        # Add attributes to message
        msg_attributes = {
                            'SourceContainer': {
                                'DataType': 'String',
                                'StringValue': self.__container_name
                            },
                            'FromQueue': {
                                'DataType': 'String',
                                'StringValue': self.__input_queue
                            },
                            'ToQueue': {
                                'DataType': 'String',
                                'StringValue': destination_queue_name
                            }
                        }

        # Send message to SQS queue
        response = client.send_message(
            QueueUrl=destination_queue_url,
            DelaySeconds=1,
            MessageAttributes= msg_attributes,
            MessageBody=str(data)
        )

        logging.info("[{}]  Message sent to {} queue".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), destination_queue_name))

    def connect_to_db(self, resource, data, attributes):
        """Connects to the DynamoDB table and checks if an item with an id equal to the file name already exists:
        
        - If yes, updates some of the item parameters with new values provided as inputs (data and attributes)
        - If not, creates a new item with the values provided in the data and attributes inputs
        
        Arguments:
            resource {boto3.resource} -- [service resource used to access the DynamoDB service]
            data {dict} -- [dict containing the info to be sent in the message body]
            attributes {dict} -- [dict containing the received message attributes (to check its contents, please refer to the msg_attributes dict structure created in the send_message function)]
        """
        
        # Select table to use
        table = resource.Table(self.__db_table_name)
        
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
                        'from_container': self.__container_name,
                        's3_path': data['s3_path'],
                        'data_status':  data['data_status'],
                        'info_source': attributes['SourceContainer']['StringValue'],
                        'processing_list': data['processing_steps'], 
                        'last_updated': str(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
                    }
            table.put_item(Item=item_db)
            logging.info("[{}]  DB item (Id: {}) created!".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), unique_id))

    def download_file(self, client, s3_bucket, file_path):
        """Retrieves a given file from the selected s3 bucket
        
        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            s3_bucket {string} -- [name of the source s3 bucket]
            file_path {string} -- [string containg the path + file name of the target file to be downloaded from the source s3 bucket (e.g. 'uber/test_file_s3.txt')]
        Returns:
            object_file {bytes} -- [downloaded file in bytes format]
        """

        logging.info("[{}]  Downloading file from S3 bucket (path: {})..".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), s3_bucket+'/'+file_path))
        
        response = client.get_object(
                                        Bucket=s3_bucket,
                                        Key=file_path
                                    )

        # Read all bytes from http response body (botocore.response.StreamingBody)
        object_file = response['Body'].read()

        logging.info("[{}]  Download completed!".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))

        return object_file

    def upload_file(self, client, object_body, s3_bucket, key_path):
        """Stores a given file in the selected s3 bucket
        
        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            object_body {bytes} -- [file to be uploaded to target S3 bucket]
            s3_bucket {string} -- [name of the destination s3 bucket]
            key_path {string} -- [string containg the path + file name to be used for the file in the destination s3 bucket (e.g. 'uber/test_file_s3.txt')]
        """

        logging.info("[{}]  Uploading file to S3 bucket (path: {})..".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"), s3_bucket+'/'+key_path))
        
        response = client.put_object(
                                        Body=object_body,
                                        Bucket=s3_bucket,
                                        Key=key_path,
                                        ServerSideEncryption='aws:kms'
                                        )

        logging.info("[{}]  Upload completed!".format(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")))