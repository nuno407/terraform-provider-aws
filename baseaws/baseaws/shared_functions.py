"""Shared functions class for communication with AWS services"""
import json
import logging
import os
import subprocess
from datetime import datetime  # timedelta as td,
from signal import SIGTERM, Signals, signal

import boto3
import pytz
from pymongo import MongoClient
from pymongo.database import Database

_logger = logging.getLogger(__name__)
VIDEO_FORMATS = {'mp4','avi'}
IMAGE_FORMATS = {'jpeg','jpg','png'}

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
        self.__sdr_folder = {}
        self.__sdr_blacklist = {}
        self.__rcc_info = {}
        self.__ivs_api = {}
        self.__docdb_config = {}

        # Container info
        self.__container = {'name': container, 'version': version}

        # Bucket and path for the config file #'dev-rcd-config-files'
        self.__config = {
            'bucket': os.environ['CONFIG_S3'],
            'file': 'containers/config_file_containers.json'
        }
        self.__secretmanagers = {}  # To be modify on dated 16 Jan'2022

        self.__apiendpoints = {}  # To be modify on dated 16 Jan'2022

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

    @property
    def sdr_folder(self):
        """sdr_folder variable"""
        return self.__sdr_folder

    @property
    def sdr_blacklist(self):
        """sdr_blacklist variable"""
        return self.__sdr_blacklist

    @property
    def rcc_info(self):
        """rcc_info variable"""
        return self.__rcc_info

    @property
    def ivs_api(self):
        """ivs_api variable"""
        return self.__ivs_api

    @property
    def docdb_config(self):
        """docdb_config variable"""
        return self.__docdb_config

    @property
    def db_tables(self):
        """db_tables variable"""
        return self.__db_tables

    @property
    def secret_managers(self):
        """ Secret Manager variable """
        return self.__secretmanagers  # To be modify on dated 16 Jan'2022

    @property
    def api_endpoints(self):
        """ API End Points variable """
        return self.__apiendpoints  # To be modify on dated 16 Jan'2022

    @property
    def time_format(self):
        """ Time format """
        return "%Y-%m-%dT%H:%M:%S.%fZ"

    def load_config_vars(self, client):
        """Gets configuration json file from s3 bucket and initialises the
        respective class variables based on the info from that file

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
        """
        full_path = self.__config['bucket']+'/'+self.__config['file']
        _logger.info("Loading parameters from: %s ..", full_path)

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

        # Name of the Raw S3 bucket folders where to store RCC KVS clips
        self.__sdr_folder = dict_body['sdr_dest_folder']

        # Dictionary containing the tenant blacklists for processing and storage of RCC clips
        self.__sdr_blacklist = dict_body['sdr_blacklist_tenants']

        # Information of the RCC account for cross-account access of services/resources
        self.__rcc_info = dict_body['rcc_info']

        # Information of the ivs_chain endpoint (ip, port, endpoint name)
        self.__ivs_api = dict_body['ivs_api']

        # To be modify on dated 16 Jan'2022
        self.__secretmanagers = dict_body['secret_manager']

        # To be modify on dated 16 Jan'2022
        self.__apiendpoints = dict_body['api_endpoints']

        # Documentdb information for client login
        self.__docdb_config = dict_body['docdb_config']

        _logger.info("Load complete!\n")

    ##### SQS related functions ########################################################
    @staticmethod
    def get_sqs_queue_url(client, queue_name):
        """Retrieves the URL for a given SQS queue

        Arguments:
            client {boto3.client} -- [client used to acces£s the SQS service]
            queue_name {string} -- [Name of the SQS queue]
        Returns:
            queue_url {string} -- [URL of the SQS queue]
        """
        #_logger.debug('Getting queue URL for %s', queue_name)
        # Request for queue url
        response = client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        #_logger.debug('Got queue URL for %s', queue_name)

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
        #_logger.debug('Receiving input message...')
        response = client.receive_message(
            QueueUrl=input_queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
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
            _logger.info("Message [%s] received!", message['MessageId'])
            _logger.debug("-> queue:  %s", input_queue)
        else:
           # _logger.debug('No Message received.')
           pass

        return message

    @staticmethod
    def configure_logging(component_name: str)->logging.Logger:
        str_level = os.environ.get('LOGLEVEL', 'INFO')
        log_level = logging.getLevelName(str_level)
        str_root_level = os.environ.get('ROOT_LOGLEVEL', 'INFO')
        log_root_level = logging.getLevelName(str_root_level)

        logging.basicConfig(format='%(asctime)s %(name)s\t%(levelname)s\t%(message)s', level=log_root_level)
        logging.getLogger('baseaws').setLevel(log_level)
        logger = logging.getLogger(component_name)
        logger.setLevel(log_level)
        return logger


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
        _logger.debug('Deleting message [%s]', receipt_handle)
        client.delete_message(
                                QueueUrl=input_queue_url,
                                ReceiptHandle=receipt_handle
                            )
        _logger.info('Deleted message [%s]', receipt_handle)

    def update_message_visibility(self, client, receipt_handle, seconds, input_queue=None):
        """ Updates the visibility timeout of an SQS message to allow longer processing
            Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            receipt_handle {string} -- [Receipt that identifies the received
                                        message to be modified]
            seconds {int} -- [new visibility timeout starting from now on]
            input_queue {string} -- [Name of the input queue the message is on.
                                     If None, the default input queue
                                     (defined in self.__queues['input']) is
                                     used instead]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues['input']

        input_queue_url = self.get_sqs_queue_url(client, input_queue)

        # Delete received message
        client.change_message_visibility(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds
        )

        _logger.info("Listening to input queue(s)..")

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
        _logger.debug('Sending message to [%s]', dest_queue)
        client.send_message(
                                        QueueUrl=destination_queue_url,
                                        DelaySeconds=1,
                                        MessageAttributes=msg_attributes,
                                        MessageBody=str(data)
                                       )
        _logger.info("Message sent to [%s]", dest_queue)

    @staticmethod
    def get_message_body(message):
        body_string = message.get('Body')
        if body_string:
            body = json.loads(body_string.replace("\'", "\""))
            return body
        else:
            return None

    ##### DB related functions #########################################################
    @staticmethod
    def get_db_connstring():
        connstring = os.environ.get('FIFTYONE_DATABASE_URI')
        return connstring

    @staticmethod
    def create_db_client() -> Database:
        """Creates MongoDB client and returns a DB object based on
        the set configurations so that a user can access the respective
        MongoDB resource

        Returns:
            db {MongoDB database object} -- [Object that can be used to
                                             access a given database
                                             and its collections]
        """
        connection_string = ContainerServices.get_db_connstring()

        # Create a MongoDB client and open the connection to MongoDB
        client = MongoClient(connection_string)

        # Specify the database to be used
        db = client["DataIngestion"]

        return db

    ##### S3 related functions ####################################################################
    @staticmethod
    def download_file(client, s3_bucket: str, file_path:str)->bytearray:
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
        full_path = s3_bucket+'/'+file_path
        _logger.debug("Downloading [%s]..", full_path)

        response = client.get_object(
            Bucket=s3_bucket,
            Key=file_path
        )

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        object_file = response['Body'].read()

        _logger.info("Downloaded [%s]", full_path)

        return object_file

    @staticmethod
    def upload_file(client, object_body, s3_bucket, key_path):
        """Stores a given file in the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            object_body {bytes} -- [file to be uploaded to target S3 bucket]
            s3_bucket {string} -- [name of the destination s3 bucket]
            key_path {string} -- [string containg the path + file name to be
                                  used for the file in the destination s3
                                  bucket (e.g. 'uber/test_file_s3.txt')]
        """
        full_path = s3_bucket+'/'+key_path
        _logger.debug("Uploading [%s]..", full_path)

        # TODO: ADD THIS INFO TO CONFIG FILE
        type_dict = {
            "json": "application/json",
            "mp4": "video/mp4",
            "avi": "video/x-msvideo",
            "txt": "text/plain",
            "webm": "video/webm",
            'jpeg': 'image/jpeg'
        }
        file_extension = key_path.split('.')[-1]

        client.put_object(
            Body=object_body,
            Bucket=s3_bucket,
            Key=key_path,
            ServerSideEncryption='aws:kms',
            ContentType=type_dict[file_extension]
        )

        _logger.info("Uploaded [%s]", key_path)

    def update_pending_queue(self, client, uid, mode, dict_body=None):
        """Inserts a new item on the algorithm output collection and, if
        there is a CHC file available for that item, processes
        that info and adds it to the item (plus updates the respective
        recordings collection item with that CHC info)

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            uid {string} -- [string containg an identifier used only in the
                             processing containers to keep track of the
                             process of a given file]
            mode {string} -- [The working mode of the current function will
                              vary based on the value stated in this parameter.
                              If mode equals to:
                               -> insert: a pending order will be added to the
                                          pending queue file based on the uid
                                          and dict_body info provided
                               -> read:  a pending order will be read and
                                         retrieved from the
                                         pending queue file based on the uid
                                         provided
                               -> delete:  a pending order will be deleted from
                                           the pending queue file based on the
                                           uid provided]
            dict_body {dict} -- [Info regarding the pending order that is
                                 going to be added to the pending queue.
                                 NOTE: Only required if mode parsed is "insert"]

        Returns:
            relay_data {dict} -- [The value returned in this variable depends
                                  on the mode used on the current function call,
                                  i.e., it will return {} (empty dictionary)
                                  for the "insert" and "delete" modes, or a
                                  pending order for the "read" mode]
        """
        # Define key s3 paths for each container's pending queue json file
        # TODO: ADD THIS INFO TO CONFIG FILE
        key_paths = {
            "Anonymize": "containers/pending_queue_anonymize.json",
            "CHC": "containers/pending_queue_chc.json"
        }

        # Download pending queue json file
        response = client.get_object(
            Bucket=self.__config['bucket'],
            Key=key_paths[self.__container['name']]
        )

        # Decode and convert received bytes into json format
        result_info = json.loads(response['Body'].read().decode("utf-8"))

        # Initialise response as empty dictionary
        relay_data = {}

        ## NOTE: Mode Selection ##

        if mode == "insert":
            # Create current time timestamp to add to pending item info (for debug)
            curr_time = str(datetime.now(
                tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S'))

            # Insert a new pending order on the downloaded file
            result_info[uid] = {
                "relay_list": dict_body,
                "creation_date": curr_time
            }

        elif mode == "read":
            # Read stored info for a given uid
            relay_data = result_info[uid]["relay_list"]

        elif mode == "delete":
            # Delete stored info for a given uid
            del result_info[uid]

        else:
            _logger.error("\nWARNING: Operation (%s) not supported!!\n", mode)

        # Encode and convert updated json into bytes to be uploaded
        object_body = json.dumps(
            result_info, indent=4, sort_keys=True).encode('utf-8')

        # Upload updated json file
        client.put_object(
            Body=object_body,
            Bucket=self.__config['bucket'],
            Key=key_paths[self.__container['name']],
            ServerSideEncryption='aws:kms',
            ContentType="application/json"
        )

        _logger.info("S3 Pending queue updated (mode: %s | uid: %s)!", mode, uid)

        return relay_data

    ##### Kinesis related functions ###############################################################
    def get_kinesis_clip(self, creds, stream_name, start_time, end_time, selector):
        """Retrieves a given chunk from the selected Kinesis video stream

        Arguments:
            creds {dict} -- [cross-account credentials to assume IAM role]
            stream_name {string} -- [name of the source Kinesis video stream]
            start_time {datetime} -- [starting timestamp of the desired clip]
            end_time {datetime} -- [ending timestamp of the desired clip]
            selector {string} -- [string containg the origin of the timestamps
                                  (can only be either 'PRODUCER_TIMESTAMP' or
                                  'SERVER_TIMESTAMP')]
        Returns:
            output_video {bytes} -- [Received chunk in bytes format]
        """
        # Generate processing start message
        _logger.info("Downloading test clip from stream [%s]..", stream_name)

        # Create Kinesis client
        kinesis_client = boto3.client('kinesisvideo',
                                      region_name='eu-central-1',
                                      aws_access_key_id=creds['AccessKeyId'],
                                      aws_secret_access_key=creds['SecretAccessKey'],
                                      aws_session_token=creds['SessionToken'])

        # Getting endpoint URL for LIST_FRAGMENTS
        response_list = kinesis_client.get_data_endpoint(StreamName=stream_name,
                                                         APIName='LIST_FRAGMENTS')

        # Getting endpoint URL for GET_MEDIA_FOR_FRAGMENT_LIST
        response_get = kinesis_client.get_data_endpoint(StreamName=stream_name,
                                                        APIName='GET_MEDIA_FOR_FRAGMENT_LIST')

        # Fetch DataEndpoint field
        endpoint_response_list = response_list['DataEndpoint']
        endpoint_response_get = response_get['DataEndpoint']

        ### List fragments step ###
        # Create Kinesis archive media client for list_fragments()
        list_client = boto3.client('kinesis-video-archived-media',
                                   endpoint_url=endpoint_response_list,
                                   region_name='eu-central-1',
                                   aws_access_key_id=creds['AccessKeyId'],
                                   aws_secret_access_key=creds['SecretAccessKey'],
                                   aws_session_token=creds['SessionToken'])

        # Get all fragments within timestamp range (start_time, end_time)
        response1 = list_client.list_fragments(
            StreamName=stream_name,
            MaxResults=1000,
            FragmentSelector={
                'FragmentSelectorType': selector,
                'TimestampRange': {
                    'StartTimestamp': start_time,
                    'EndTimestamp': end_time
                }
            }
        )

        _logger.debug('Got fragments for test clip on [%s]', stream_name)

        # Sort fragments by their timestamp
        newlist = sorted(response1['Fragments'], key=lambda d: datetime.timestamp(
            (d['ProducerTimestamp'])))

        # Create comprehension list with sorted fragments
        list_frags = [frag['FragmentNumber'] for frag in newlist]

        ### Get media step ###
        # Create Kinesis archive media client for get_media_for_fragment_list()
        get_client = boto3.client('kinesis-video-archived-media',
                                  endpoint_url=endpoint_response_get,
                                  region_name='eu-central-1',
                                  aws_access_key_id=creds['AccessKeyId'],
                                  aws_secret_access_key=creds['SecretAccessKey'],
                                  aws_session_token=creds['SessionToken'])

        # Fetch all 2sec fragments from previously sorted list
        response2 = get_client.get_media_for_fragment_list(
            StreamName=stream_name,
            Fragments=list_frags
        )

        _logger.debug('Got video file for test clip on [%s]', stream_name)

        ### Conversion step (webm -> mp4) ###
        # Defining temporary files names
        input_name = "received_file.webm"
        output_name = "converted_file.mp4"
        logs_name = "logs.txt"

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        video_chunk = response2['Payload'].read()

        # Save video chunks to file
        with open(input_name, 'wb') as infile:
            infile.write(video_chunk)

        with open(logs_name, 'w') as logs_write:
            # Convert .avi input file into .mp4 using ffmpeg
            _logger.debug('Starting ffmpeg conversion')
            conv_logs = subprocess.Popen(["ffmpeg", "-i", input_name, "-qscale",
                                        "0", "-filter:v", "fps=15.72", output_name],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT,
                                        universal_newlines=True)
            _logger.debug('Finished ffmpeg conversion')

            # Save conversion logs into txt file
            for line in conv_logs.stdout:
                logs_write.write(line)

        # Load bytes from converted output file
        with open(output_name, "rb") as output_file:
            output_video = output_file.read()

        # Remove temporary files from storage
        subprocess.run(["rm", input_name, output_name, logs_name])

        # Generate processing end message
        _logger.info("Test clip download completed!")

        return output_video

    ##### Logs/Misc. functions ####################################################################
    @staticmethod
    def display_processed_msg(key_path, uid=None):
        """Displays status message for processing completion

        Arguments:
            key_path {string} -- [string containg the path + name of the file,
                                  whose processing status is being updated to
                                  completed (e.g. 'uber/test_file_s3.txt')]
            uid {string} -- [string containg an identifier used only in the
                             processing containers to keep track of the
                             process of a given file. Optional]
        """
        _logger.info("\nProcessing of key [%s] complete! (uid: [%s])", key_path, uid)


# inspired by https://stackoverflow.com/a/31464349
class GracefulExit:
    @property
    def continue_running(self):
        return self.__continue_running

    def __init__(self):
        self.__continue_running = True
        signal(SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        _logger.critical("Received termination request with signal %s. Trying to shutdown gracefully.", Signals(signum).name)
        self.__continue_running = False


class StsHelper:
    def __init__(self, sts_client, role: str, role_session: str) -> None:
        self.__role = role
        self.__role_session = role_session
        self.__client = sts_client
        self.__renew_credentials()

    def __renew_credentials(self):
        assumed_role = self.__client.assume_role(
            RoleArn=self.__role, RoleSessionName=self.__role_session)
        self.__credentials = assumed_role['Credentials']
        self.__last_renew = datetime.now()

    def get_credentials(self) -> dict:
        if (datetime.now() - self.__last_renew).total_seconds() > 1800:
            self.__renew_credentials()
        return self.__credentials


class AWSServiceClients():

    def __init__(self, sqs_client: boto3.Session.client, s3_client: boto3.Session.client):
        self.sqs_client = sqs_client
        self.s3_client = s3_client
