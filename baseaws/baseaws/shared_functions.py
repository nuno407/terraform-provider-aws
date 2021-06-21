"""Shared functions class for communication with AWS services"""
import json
import logging
import os
from datetime import datetime
import pytz


class ContainerServices():
    """ContainerServices

    Class container comprised of all the necessary tools to establish
    connection and interact with the various AWS services
    """

    def __init__(self, container, version):
        # Config variables
        self.__queues = {'list': {}, 'input': ""}
        self.__msp_steps = {}
        self.__db_table = ""
        self.__s3_buckets = {'raw': "", 'anonymized': ""}

        # Container info
        self.__container = {'name': container, 'version': version}

        # Time format
        self.__time_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        # Bucket and path for the config file
        self.__config = {
                          'bucket': 'dev-rcd-config-files',
                          'file': 'containers/config_file_containers.json'
                        }

        # Define configuration for logging messages
        logging.basicConfig(format='%(message)s', level=logging.INFO)

    @property
    def sqs_queues_list(self):
        """sqs_queues_list variable"""
        return self.__queues['list']

    @property
    def input_queue(self):
        """input_queue variable"""
        return self.__queues['input']

    @property
    def msp_steps(self):
        """msp_steps variable"""
        return self.__msp_steps

    @property
    def raw_s3(self):
        """raw_s3 variable"""
        return self.__s3_buckets['raw']

    @property
    def anonymized_s3(self):
        """anonymized_s3 variable"""
        return self.__s3_buckets['anonymized']

    def load_config_vars(self, client):
        """Gets configuration json file from s3 bucket and initialises the
        respective class variables based on the info from that file

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
        """
        full_path = self.__config['bucket']+'/'+self.__config['file']
        logging.info("Loading parameters from: %s ..", full_path)

        # Send request to access the config file (json)
        response = client.get_object(
            Bucket=self.__config['bucket'],
            Key=self.__config['file']
        )

        # Load config file (botocore.response.StreamingBody)
        # content to dictionary
        dict_body = json.loads(response['Body'].read().decode("utf-8"))

        # List of the queues attached to each container
        self.__queues['list'] = dict_body['sqs_queues_list']

        # Name of the input queue attached to the current container
        self.__queues['input'] = self.__queues['list'][self.__container['name']]

        # List of processing steps required for each file based on the MSP
        self.__msp_steps = dict_body['msp_processing_steps']

        # Name of the dynamoDB table used to store metadata
        self.__db_table = dict_body['db_metadata_table']

        # Name of the S3 bucket used to store raw video files
        self.__s3_buckets['raw'] = dict_body['s3_buckets']['raw']

        # Name of the S3 bucket used to store anonymized video files
        self.__s3_buckets['anonymized'] = dict_body['s3_buckets']['anonymized']

        logging.info("Load complete!\n")

    @staticmethod
    def get_sqs_queue_url(client, queue_name):
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
        """Logs into the input SQS queue of a given container
        and checks for new messages.

        - If the queue is empty, it waits up until 20s for
        new messages before continuing

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
        Returns:
            message {dict} -- [dict with the received message content
                               (for more info please check the response syntax
                               of the Boto3 SQS.client.receive_message method).
                               If no message is received, returns None]
        """
        # Get URL for container input SQS queue
        input_queue_url = self.get_sqs_queue_url(client, self.__queues['input'])

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

        # Initialise message variable with default value
        message = None

        # If queue has new messages
        if 'Messages' in response:
            # Select the first message received
            # (by default, it only receives 1 message
            # per enquiry - set above by MaxNumberOfMessages parameter)
            message = response['Messages'][0]
            timestamp = datetime.now(
                                    tz=pytz.UTC
                                    ).strftime(self.__time_format)
            logging.info("-----------------------------------------------")
            logging.info("Message received!")
            logging.info("    -> id:  %s", message['MessageId'])
            logging.info("    -> timestamp: %s\n", str(timestamp))

        return message

    def delete_message(self, client, receipt_handle):
        """Deletes received SQS message

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            receipt_handle {string} -- [Receipt that identifies the received
                                        message to be deleted]
        """
        # Get URL for container input SQS queue
        input_queue_url = self.get_sqs_queue_url(client, self.__queues['input'])

        # Delete received message
        client.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle
        )

        logging.info("-----------------------------------------------")
        logging.info("\n\n")
        logging.info("Listening to %s queue..\n\n", self.__queues['input'])

    def send_message(self, client, dest_queue, data):
        """Prepares the message attributes + body and sends a message
        with that information to the target queue

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            dest_queue {string} -- [Name of the
                                    destination output SQS queue]
            data {dict} -- [dict containing the info to be
                            sent in the message body]
        """
        # Get URL for target output SQS queue
        destination_queue_url = self.get_sqs_queue_url(client,
                                                       dest_queue)

        # Add attributes to message
        msg_attributes = {
                            'SourceContainer': {
                                'DataType': 'String',
                                'StringValue': self.__container['name']
                            },
                            'FromQueue': {
                                'DataType': 'String',
                                'StringValue': self.__queues['input']
                            },
                            'ToQueue': {
                                'DataType': 'String',
                                'StringValue': dest_queue
                            }
                        }

        # Send message to SQS queue
        response = client.send_message(
            QueueUrl=destination_queue_url,
            DelaySeconds=1,
            MessageAttributes=msg_attributes,
            MessageBody=str(data)
        )

        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        logging.info("[%s]  Message sent to %s queue", str(timestamp),
                                                       dest_queue)

    def connect_to_db(self, resource, data, attributes):
        """Connects to the DynamoDB table and checks if an item
        with an id equal to the file name already exists:

        - If yes, updates some of the item parameters with
        new values provided as inputs (data and attributes)
        - If not, creates a new item with the values provided
        in the data and attributes inputs

        Arguments:
            resource {boto3.resource} -- [service resource used to
                                          access the DynamoDB service]
            data {dict} -- [dict containing the info to be sent
                            in the message body]
            attributes {dict} -- [dict containing the received message
                                  attributes (to check its contents,
                                  please refer to the msg_attributes
                                  dict structure created in the
                                  send_message function)]
        """
        # Select table to use
        table = resource.Table(self.__db_table)

        # Get filename (id) from message received
        unique_id = os.path.basename(data["s3_path"]).split(".")[0]

        # Check if item with that name already exists
        response = table.get_item(Key={'id': unique_id})
        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        if 'Item' in response:
            # Update already existing item
            table.update_item(
                              Key={'id': unique_id},
                              UpdateExpression='SET data_status = :val1, info_source = :val2, last_updated = :val3',
                              ExpressionAttributeValues={
                                                         ':val1': data['data_status'],
                                                         ':val2': attributes['SourceContainer']['StringValue'],
                                                         ':val3': str(timestamp)
                                                        },
                              ReturnValues="UPDATED_NEW"
                            )
            logging.info("[%s]  DB item (Id: %s) updated!", str(timestamp),
                                                            unique_id)
        else:
            # Insert item if not created yet
            item_db = {
                        'id': unique_id,
                        'from_container': self.__container['name'],
                        's3_path': data['s3_path'],
                        'data_status':  data['data_status'],
                        'info_source': attributes['SourceContainer']['StringValue'],
                        'processing_list': data['processing_steps'],
                        'last_updated': str(timestamp)
                    }
            table.put_item(Item=item_db)
            logging.info("[%s]  DB item (Id: %s) created!", str(timestamp),
                                                            unique_id)

    def download_file(self, client, s3_bucket, file_path):
        """Retrieves a given file from the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            s3_bucket {string} -- [name of the source s3 bucket]
            file_path {string} -- [string containg the path + file name of
                                   the target file to be downloaded
                                   from the source s3 bucket
                                   (e.g. 'uber/test_file_s3.txt')]
        Returns:
            object_file {bytes} -- [downloaded file in bytes format]
        """
        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        full_path = s3_bucket+'/'+file_path
        logging.info("[%s]  Downloading file (path: %s)..", str(timestamp),
                                                            full_path)

        response = client.get_object(
                                        Bucket=s3_bucket,
                                        Key=file_path
                                    )

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        object_file = response['Body'].read()

        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        logging.info("[%s]  Download completed!", str(timestamp))

        return object_file

    def upload_file(self, client, object_body, s3_bucket, key_path):
        """Stores a given file in the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            object_body {bytes} -- [file to be uploaded to target S3 bucket]
            s3_bucket {string} -- [name of the destination s3 bucket]
            key_path {string} -- [string containg the path + file name to be
                                used for the file in the destination s3 bucket
                                (e.g. 'uber/test_file_s3.txt')]
        """
        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        full_path = s3_bucket+'/'+key_path
        logging.info("[%s]  Uploading file (path: %s)..", str(timestamp),
                                                          full_path)

        response = client.put_object(
                                        Body=object_body,
                                        Bucket=s3_bucket,
                                        Key=key_path,
                                        ServerSideEncryption='aws:kms'
                                        )

        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        logging.info("[%s]  Upload completed!", str(timestamp))

    def display_processed_msg(self, key_path):
        """Displays status message for processing completion

        Arguments:
            key_path {string} -- [string containg the path + name of the file,
                                whose processing status is being updated to
                                completed (e.g. 'uber/test_file_s3.txt')]
        """
        timestamp = datetime.now(tz=pytz.UTC).strftime(self.__time_format)
        logging.info("\nProcessing complete!")
        logging.info("    -> key: %s", key_path)
        logging.info("    -> timestamp: %s\n", str(timestamp))
