"""Shared functions class for communication with AWS services"""
import json
import logging
import os
from datetime import datetime
import pytz
import boto3

class ContainerServices():
    """ContainerServices

    Class container comprised of all the necessary tools to establish
    connection and interact with the various AWS services
    """

    def __init__(self, container, version):
        # Config variables
        self.__queues = {'list': {}, 'input': ""}
        self.__msp_steps = {}
        self.__db_tables = {}
        self.__s3_buckets = {'raw': "", 'anonymized': ""}
        self.__s3_ignore = {'raw': "", 'anonymized': ""}

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

    @property
    def raw_s3_ignore(self):
        """raw_s3_ignore variable"""
        return self.__s3_ignore['raw']

    @property
    def anonymized_s3_ignore(self):
        """anonymized_s3_ignore variable"""
        return self.__s3_ignore['anonymized']

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
        self.__db_tables = dict_body['db_metadata_tables']

        # Name of the S3 bucket used to store raw video files
        self.__s3_buckets['raw'] = dict_body['s3_buckets']['raw']

        # Name of the S3 bucket used to store anonymized video files
        self.__s3_buckets['anonymized'] = dict_body['s3_buckets']['anonymized']

        # List of all file formats that should be ignored by
        # the processing container
        self.__s3_ignore['raw'] = dict_body['s3_ignore_list']['raw']
        self.__s3_ignore['anonymized'] = dict_body['s3_ignore_list']['anonymized']

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

    def listen_to_input_queue(self, client, input_queue=None):
        """Logs into the input SQS queue of a given container
        and checks for new messages.

        - If the queue is empty, it waits up until 20s for
        new messages before continuing

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            input_queue {string} -- [Name of the input queue to listen to.
                                     If None, the default input queue
                                     (defined in self.__queues['input']) is
                                     used instead]
        Returns:
            message {dict} -- [dict with the received message content
                               (for more info please check the response syntax
                               of the Boto3 SQS.client.receive_message method).
                               If no message is received, returns None]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues['input']

        input_queue_url = self.get_sqs_queue_url(client, input_queue)

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
            timestamp = str(datetime.now(
                                        tz=pytz.UTC
                                        ).strftime(self.__time_format))
            logging.info("\n-----------------------------------------------")
            logging.info("Message received!")
            logging.info("-> id:  %s", message['MessageId'])
            logging.info("-> queue:  %s", input_queue)
            logging.info("-> timestamp: %s\n", timestamp)

        return message

    def delete_message(self, client, receipt_handle, input_queue=None):
        """Deletes received SQS message

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            receipt_handle {string} -- [Receipt that identifies the received
                                        message to be deleted]
            input_queue {string} -- [Name of the input queue to delete from.
                                     If None, the default input queue
                                     (defined in self.__queues['input']) is
                                     used instead]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues['input']

        input_queue_url = self.get_sqs_queue_url(client, input_queue)


        # Delete received message
        client.delete_message(
                                QueueUrl=input_queue_url,
                                ReceiptHandle=receipt_handle
                            )

        logging.info("-----------------------------------------------")
        logging.info("\n\nListening to input queue(s)..\n")

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

        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Message sent to %s queue", timestamp,
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
        # Select tables to use
        table_pipe = resource.Table(self.__db_tables['pipeline_exec'])
        table_algo_out = resource.Table(self.__db_tables['algo_output'])

        # Get filename (id) from message received
        unique_id = os.path.basename(data["s3_path"]).split(".")[0]

        # Initialise variables used in both item creation and update
        status = data['data_status']
        source = attributes['SourceContainer']['StringValue']

        # Check if item with that name already exists
        response = table_pipe.get_item(Key={'id': unique_id})
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))

        # Create/Update item on Pipeline Execution DB
        if 'Item' in response:
            # Build update expression
            exp1 = 'data_status = :val1'
            exp2 = 'info_source = :val2'
            exp3 = 'last_updated = :val3'
            db_expression = 'SET '+exp1+', '+exp2+', '+exp3

            # Update already existing item
            table_pipe.update_item(
                                   Key={'id': unique_id},
                                   UpdateExpression=db_expression,
                                   ExpressionAttributeValues={
                                                               ':val1': status,
                                                               ':val2': source,
                                                               ':val3': timestamp
                                                               },
                                   ReturnValues="UPDATED_NEW"
                                   )
            logging.info("[%s]  Pipeline Exec DB item (Id: %s) updated!", timestamp,
                                                                          unique_id)
        else:
            # Insert item if not created yet
            item_db = {
                        'id': unique_id,
                        'from_container': self.__container['name'],
                        's3_path': data['s3_path'],
                        'data_status': status,
                        'info_source': source,
                        'processing_list': data['processing_steps'],
                        'last_updated': timestamp
                    }
            table_pipe.put_item(Item=item_db)
            logging.info("[%s]  Pipeline Exec DB item (Id: %s) created!", timestamp,
                                                                          unique_id)

        # Create/Update item on Algorithm Output DB
        if 'metadata' in data:
            # Initialise variables used in item creation
            full_path = self.__s3_buckets['anonymized']+'/'+data['s3_path']
            run_id = source+'_'+unique_id

            # Item creation
            item_db = {
                        'results': data['metadata'],
                        'pipeline_id': unique_id,
                        's3_path': full_path,
                        'algorithm_id': source,
                        'run_id': run_id
                    }
            table_algo_out.put_item(Item=item_db)
            logging.info("[%s]  Algo Output DB item (Id: %s) created!", timestamp,
                                                                        unique_id)

        # TODO: SPLIT THIS FUNCTION WHEN WE HAVE MORE TABLES

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
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        full_path = s3_bucket+'/'+file_path
        logging.info("[%s]  Downloading file (path: %s)..", timestamp,
                                                            full_path)

        response = client.get_object(
                                        Bucket=s3_bucket,
                                        Key=file_path
                                    )

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        object_file = response['Body'].read()

        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Download completed!", timestamp)

        return object_file

    def upload_file(self, client, object_body, s3_bucket, key_path):
        """Stores a given file in the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            object_body {bytes} -- [file to be uploaded to target S3 bucket]
            s3_bucket {string} -- [name of the destination s3 bucket]
            key_path {string} -- [string containg the path + file name to be
                                  used for the file in the destination s3
                                  bucket (e.g. 'uber/test_file_s3.txt')]
        """
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        full_path = s3_bucket+'/'+key_path
        logging.info("[%s]  Uploading file (path: %s)..", timestamp,
                                                          full_path)

        response = client.put_object(
                                        Body=object_body,
                                        Bucket=s3_bucket,
                                        Key=key_path,
                                        ServerSideEncryption='aws:kms'
                                        )

        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Upload completed!", timestamp)

    def display_processed_msg(self, key_path, uid=None):
        """Displays status message for processing completion

        Arguments:
            key_path {string} -- [string containg the path + name of the file,
                                  whose processing status is being updated to
                                  completed (e.g. 'uber/test_file_s3.txt')]
            uid {string} -- [string containg an identifier used only in the
                             processing containers to keep track of the
                             process of a given file. Optional]
        """
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("\nProcessing complete!")
        logging.info("-> key: %s", key_path)
        if uid:
            logging.info("-> uid: %s", uid)
        logging.info("-> timestamp: %s\n", timestamp)

    def get_kinesis_clip(self, client, stream_name, start_time, end_time, selector):
        """Retrieves a given chunk from the selected Kinesis video stream

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            stream_name {string} -- [name of the source Kinesis video stream]
            start_time {datetime} -- [starting timestamp of the desired clip]
            end_time {datetime} -- [ending timestamp of the desired clip]
            selector {string} -- [string containg the origin of the timestamps
                                  (can only be either 'PRODUCER_TIMESTAMP' or
                                  'SERVER_TIMESTAMP')]
        Returns:
            video_clip {bytes} -- [Received chunk in bytes format]
        """
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Downloading clip (stream: %s)..", timestamp,
                                                              stream_name)

        # Getting endpoint URL for GET_CLIP
        response = client.get_data_endpoint(
            StreamName=stream_name,
            APIName='GET_CLIP'
        )

        endpoint_response = response['DataEndpoint']

        # Create client using received endpoint URL
        media_client = boto3.client('kinesis-video-archived-media',
                                    endpoint_url=endpoint_response,
                                    region_name='eu-central-1')

        # Send request to get desired clip
        response_media = media_client.get_clip(
            StreamName=stream_name,
            ClipFragmentSelector={
                'FragmentSelectorType': selector,
                'TimestampRange': {
                    'StartTimestamp': start_time,
                    'EndTimestamp': end_time
                }
            }
        )

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        video_chunk = response_media['Payload'].read()

        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Clip download completed!", timestamp)

        return video_chunk