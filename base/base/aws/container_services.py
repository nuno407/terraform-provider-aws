"""Shared functions class for communication with AWS services"""
import json
import logging
import os
import subprocess  # nosec
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Tuple

import boto3
import yaml
from botocore.errorfactory import ClientError
from expiringdict import ExpiringDict  # type: ignore
from mypy_boto3_kinesis_video_archived_media import \
    KinesisVideoArchivedMediaClient
from mypy_boto3_kinesisvideo import KinesisVideoClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef
from pymongo import MongoClient
from pymongo.database import Database

_logger = logging.getLogger(__name__)
VIDEO_FORMATS = {"mp4", "avi"}
IMAGE_FORMATS = {"jpeg", "jpg", "png"}
MAX_MESSAGE_VISIBILITY_TIMEOUT = 43200
MESSAGE_VISIBILITY_TIMEOUT_BUFFER = 0.5
DATA_INGESTION_DATABASE_NAME = "DataIngestion"


@dataclass
class RCCS3ObjectParams:
    """ Class to wrap common "s3 path" operations on RCC s3 paths. """
    s3_path: str = field()
    bucket: str = field()

    @property
    def tenant(self) -> str:
        """ Obtain tenant. """
        return self.s3_path.split("/")[0]

    @property
    def deviceid(self) -> str:
        """ Obtain deviceid. """
        return self.s3_path.split("/")[1]


class ContainerServices():  # pylint: disable=too-many-locals,missing-function-docstring,too-many-instance-attributes,too-many-public-methods
    """ContainerServices

    Class container comprised of all the necessary tools to establish
    connection and interact with the various AWS services
    """

    def __init__(self, container, version):
        # Config variables
        self.__queues = {"list": {}, "input": ""}
        self.__msp_steps = {}
        self.__db_tables = {}
        self.__s3_buckets = {"raw": "", "anonymized": ""}
        self.__s3_ignore = {"raw": [], "anonymized": []}
        self.__sdr_folder = {}
        self.__rcc_info = {}
        self.__ivs_api = {}

        # Container info
        self.__container = {"name": container, "version": version}

        # Configure vars with paths for configmaps
        self.__config = {
            "AWS_CONFIG": os.environ.get("AWS_CONFIG", "/app/aws-conf/aws_config.yaml"),
            "MONGODB_CONFIG": os.environ.get("MONGODB_CONFIG", "/app/mongo-conf/mongo_config.yaml")
        }
        self.__secretmanagers = {}  # To be modify on dated 16 Jan"2022

        self.__apiendpoints = {}  # To be modify on dated 16 Jan"2022

        self.__message_receive_times: ExpiringDict[str, datetime] = ExpiringDict(
            max_len=1000, max_age_seconds=50400)

    @property
    def sqs_queues_list(self):
        """sqs_queues_list variable"""
        return self.__queues["list"]

    @property
    def input_queue(self):
        """input_queue variable"""
        return self.__queues["input"]

    @property
    def msp_steps(self):
        """msp_steps variable"""
        return self.__msp_steps

    @property
    def raw_s3(self):
        """raw_s3 variable"""
        return self.__s3_buckets["raw"]

    @property
    def anonymized_s3(self):
        """anonymized_s3 variable"""
        return self.__s3_buckets["anonymized"]

    @property
    def raw_s3_ignore(self):
        """raw_s3_ignore variable"""
        return self.__s3_ignore["raw"]

    @property
    def anonymized_s3_ignore(self):
        """anonymized_s3_ignore variable"""
        return self.__s3_ignore["anonymized"]

    @property
    def sdr_folder(self):
        """sdr_folder variable"""
        return self.__sdr_folder

    @property
    def rcc_info(self):
        """rcc_info variable"""
        return self.__rcc_info

    @property
    def ivs_api(self):
        """ivs_api variable"""
        return self.__ivs_api

    @property
    def db_tables(self):
        """db_tables variable"""
        return self.__db_tables

    @property
    def secret_managers(self):
        """ Secret Manager variable """
        return self.__secretmanagers  # To be modify on dated 16 Jan"2022

    @property
    def api_endpoints(self):
        """ API End Points variable """
        return self.__apiendpoints  # To be modify on dated 16 Jan"2022

    @property
    def time_format(self):
        """ Time format """
        return "%Y-%m-%dT%H:%M:%S.%fZ"

    def load_config_vars(self):
        """Gets configuration yaml (mounted as volume) and initializes
        the respective class variables based on the info from that file

        """
        _logger.info("Loading aws parameters from: %s ..", self.__config["AWS_CONFIG"])
        with open(self.__config["AWS_CONFIG"], "r", encoding="utf-8") as configfile:
            dict_body = dict(yaml.safe_load(configfile).items())

        # List of the queues attached to each container
        self.__queues["list"] = dict_body["sqs_queues_list"]

        # Name of the input queue attached to the current container
        self.__queues["input"] = self.__queues["list"][self.__container["name"]]

        # List of processing steps required for each file based on the MSP
        self.__msp_steps = dict_body["msp_processing_steps"]

        # Name of the S3 bucket used to store raw video files
        self.__s3_buckets["raw"] = dict_body["s3_buckets"]["raw"]

        # Name of the S3 bucket used to store anonymized video files
        self.__s3_buckets["anonymized"] = dict_body["s3_buckets"]["anonymized"]

        # List of all file formats that should be ignored by
        # the processing container
        self.__s3_ignore["raw"] = dict_body["s3_ignore_list"]["raw"]
        self.__s3_ignore["anonymized"] = dict_body["s3_ignore_list"]["anonymized"]

        # Name of the Raw S3 bucket folders where to store RCC KVS clips
        self.__sdr_folder = dict_body["sdr_dest_folder"]

        # Information of the RCC account for cross-account access of services/resources
        self.__rcc_info = dict_body["rcc_info"]

        # Information of the ivs_chain endpoint (ip, port, endpoint name)
        self.__ivs_api = dict_body["ivs_api"]

        # Secrets manager
        self.__secretmanagers = dict_body["secret_manager"]

        # RCC API endpoints
        self.__apiendpoints = dict_body["api_endpoints"]

        _logger.info("Load complete!\n")

    def load_mongodb_config_vars(self):
        """Gets configuration yaml (mounted as volume) and initializes
        the MongoDB variable based on the info from that file

        """
        _logger.info("Loading mongodb parameters from: %s ..", self.__config["MONGODB_CONFIG"])
        with open(self.__config["MONGODB_CONFIG"], "r", encoding="utf-8") as configfile:
            dict_body = dict(yaml.safe_load(configfile).items())

        # Name of the MongoDB tables used to store metadata
        self.__db_tables = dict_body["db_metadata_tables"]

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
        # _logger.debug("Getting queue URL for %s", queue_name)
        # Request for queue url
        response = client.get_queue_url(QueueName=queue_name)
        queue_url = response["QueueUrl"]
        # _logger.debug("Got queue URL for %s", queue_name)

        return queue_url

    def get_single_message_from_input_queue(self, client, input_queue=None):
        """Uses get_multiple_messages_from_input_queue to fetch a single message

        - If the queue is empty, it waits up until 20s for
        new messages before continuing

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            input_queue {string} -- [Name of the input queue to listen to.
                                     If None, the default input queue
                                     (defined in self.__queues["input"]) is
                                     used instead]
        Returns:
            message -- [the received message content
                        (for more info please check the response syntax
                        of the Boto3 SQS.client.receive_message method).
                        If no message is received, returns None]
        """

        result = self.get_multiple_messages_from_input_queue(client, input_queue, max_number_of_messages=1)
        if len(result) == 1:
            return result[0]

        return None

    def get_multiple_messages_from_input_queue(self, client, input_queue=None, max_number_of_messages=1):
        """Logs into the input SQS queue of a given container
        and checks for new messages.

        - If the queue is empty, it waits up until 20s for
        new messages before continuing

        - Returns a batch of messages ranging from 0 to 10 messages.

        Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            input_queue {string} -- [Name of the input queue to listen to.
                                     If None, the default input queue
                                     (defined in self.__queues["input"]) is
                                     used instead]
        Returns:
            message {dict} -- [dict with the received message content
                               (for more info please check the response syntax
                               of the Boto3 SQS.client.receive_message method).
                               If no message is received, returns None]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues["input"]

        input_queue_url = self.get_sqs_queue_url(client, input_queue)

        # Receive message(s)
        response = client.receive_message(
            QueueUrl=input_queue_url,
            AttributeNames=[
                "SentTimestamp",
                "ApproximateReceiveCount"
            ],
            MaxNumberOfMessages=max_number_of_messages,
            MessageAttributeNames=[
                "All"
            ],
            WaitTimeSeconds=20
        )

        # If queue has new messages
        if "Messages" in response:
            for message in response["Messages"]:
                _logger.info("Message [%s] received!", message["MessageId"])
                _logger.debug("-> queue:  %s", input_queue)
                self.__message_receive_times[message["ReceiptHandle"]] = datetime.now()
            return response["Messages"]

        return []

    @staticmethod
    def configure_logging(component_name: str) -> logging.Logger:
        str_level = os.environ.get("LOGLEVEL", "INFO")
        log_level = logging.getLevelName(str_level)
        str_root_level = os.environ.get("ROOT_LOGLEVEL", "INFO")
        log_root_level = logging.getLevelName(str_root_level)

        logging.basicConfig(
            format="%(asctime)s %(name)s\t%(levelname)s\t%(message)s", level=log_root_level)
        logging.getLogger("base").setLevel(log_level)
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
                                     (defined in self.__queues["input"]) is
                                     used instead]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues["input"]

        input_queue_url = self.get_sqs_queue_url(client, input_queue)

        # Delete received message
        _logger.debug("Deleting message [%s]", receipt_handle)
        client.delete_message(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle
        )
        self.__message_receive_times.pop(receipt_handle, None)
        _logger.info("Deleted message [%s]", receipt_handle)

    def update_message_visibility(self, client, receipt_handle: str, seconds: int, input_queue=None):
        """ Updates the visibility timeout of an SQS message to allow longer processing
            Arguments:
            client {boto3.client} -- [client used to access the SQS service]
            receipt_handle {string} -- [Receipt that identifies the received
                                        message to be modified]
            seconds {int} -- [new visibility timeout starting from now on]
            input_queue {string} -- [Name of the input queue the message is on.
                                     If None, the default input queue
                                     (defined in self.__queues["input"]) is
                                     used instead]
        """
        if not input_queue:
            # Get URL for container default input SQS queue
            input_queue = self.__queues["input"]

        input_queue_url = self.get_sqs_queue_url(client, input_queue)

        # Limit message visibility timeout to 12h
        processing_time = int((datetime.now() - self.__message_receive_times.get(receipt_handle, datetime.now(
        ))).total_seconds() + 1.0 + MESSAGE_VISIBILITY_TIMEOUT_BUFFER)
        print(processing_time)
        if seconds > MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time:
            seconds = MAX_MESSAGE_VISIBILITY_TIMEOUT - processing_time
            logging.debug(
                "Limiting message visibility extension to %d seconds", seconds)

        # Delete received message
        client.change_message_visibility(
            QueueUrl=input_queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds
        )

        _logger.debug(
            "Changed message visibility timeout to %d seconds", seconds)

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
            "SourceContainer": {
                "DataType": "String",
                "StringValue": self.__container["name"]
            },
            "ToQueue": {
                "DataType": "String",
                "StringValue": dest_queue
            }
        }

        # Send message to SQS queue
        _logger.debug("Sending message to [%s]", dest_queue)
        client.send_message(
            QueueUrl=destination_queue_url,
            DelaySeconds=1,
            MessageAttributes=msg_attributes,
            MessageBody=str(data)
        )
        _logger.info("Message sent to [%s]", dest_queue)

    @staticmethod
    def get_message_body(message):
        body_string = message.get("Body")
        if body_string:
            body = json.loads(body_string.replace("'", "\""))
            return body
        return None

    ##### DB related functions #########################################################
    @staticmethod
    def get_db_connstring():
        connstring = os.environ.get("FIFTYONE_DATABASE_URI")
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
        client: MongoClient = MongoClient(connection_string)

        return client[DATA_INGESTION_DATABASE_NAME]

    ##### S3 related functions ####################################################################
    @staticmethod
    def download_file(client, s3_bucket: str, file_path: str) -> bytearray:
        """Retrieves a given file from the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            s3_bucket {string} -- [name of the source s3 bucket]
            file_path {string} -- [string containg the path + file name of
                                   the target file to be downloaded
                                   from the source s3 bucket
                                   (e.g. "uber/test_file_s3.txt")]
        Returns:
            object_file {bytes} -- [downloaded file in bytes format]
        """
        full_path = s3_bucket + "/" + file_path
        _logger.debug("Downloading [%s]..", full_path)
        response = client.get_object(
            Bucket=s3_bucket,
            Key=file_path
        )

        # Read all bytes from http response body
        # (botocore.response.StreamingBody)
        object_file = response["Body"].read()

        _logger.debug("Downloaded [%s]", full_path)

        return object_file

    @staticmethod
    def download_file_to_disk(client, s3_bucket: str, file_path: str, stored_file: str):
        """Retrieves a given file from the selected s3 bucket and stores it to disk

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            s3_bucket {string} -- [name of the source s3 bucket]
            file_path {string} -- [string containing the path + file name of
                                   the target file to be downloaded
                                   from the source s3 bucket
                                   (e.g. "uber/test_file_s3.txt")]
            stored_file {string} -- [string containing the full path where
                                     to download the file
                                     (e.g. "~/abc/test_file_s3.txt")]
        """
        full_path = s3_bucket + "/" + file_path
        _logger.debug("Downloading to disk from [%s]..", full_path)
        client.download_file(
            Bucket=s3_bucket,
            Key=file_path,
            Filename=stored_file
        )

        _logger.debug("Downloaded to [%s]", stored_file)

    @staticmethod
    def upload_file(client, object_body, s3_bucket, key_path, log_level=logging.INFO):
        """Stores a given file in the selected s3 bucket

        Arguments:
            client {boto3.client} -- [client used to access the S3 service]
            object_body {bytes} -- [file to be uploaded to target S3 bucket]
            s3_bucket {string} -- [name of the destination s3 bucket]
            key_path {string} -- [string containg the path + file name to be
                                  used for the file in the destination s3
                                  bucket (e.g. "uber/test_file_s3.txt")]
        """
        full_path = s3_bucket + "/" + key_path
        _logger.debug("Uploading [%s]..", full_path)

        # ADD THIS INFO TO CONFIG FILE
        type_dict = {
            "json": "application/json",
            "mp4": "video/mp4",
            "avi": "video/x-msvideo",
            "txt": "text/plain",
            "webm": "video/webm",
            "jpeg": "image/jpeg",
            "jpg": "image/jpeg",
            "csv": "text/plain",
            "pickle": "application/octet-stream",
            "png": "image/png"
        }
        file_extension = key_path.split(".")

        request = {
            "Body": object_body,
            "Bucket": s3_bucket,
            "Key": key_path,
            "ServerSideEncryption": "aws:kms"
        }
        if len(file_extension) == 2:
            request["ContentType"] = type_dict[file_extension[-1]]

        client.put_object(**request)

        _logger.log(log_level, "Uploaded [%s]", key_path)

    ##### Kinesis related functions ###############################################################
    def get_kinesis_clip(self, creds, stream_name, start_time, end_time, selector) -> Tuple[bytes, datetime, datetime]:  # pylint: disable=too-many-arguments
        """Retrieves a given chunk from the selected Kinesis video stream

        Arguments:
            creds {dict} -- [cross-account credentials to assume IAM role]
            stream_name {string} -- [name of the source Kinesis video stream]
            start_time {datetime} -- [starting timestamp of the desired clip]
            end_time {datetime} -- [ending timestamp of the desired clip]
            selector {string} -- [string containg the origin of the timestamps
                                  (can only be either "PRODUCER_TIMESTAMP" or
                                  "SERVER_TIMESTAMP")]
        Returns:
            output_video {bytes} -- [Received chunk in bytes format]
        """
        # Generate processing start message
        _logger.info("Downloading test clip from stream [%s]..", stream_name)

        # Create Kinesis client
        kinesis_client: KinesisVideoClient = boto3.client("kinesisvideo",
                                                          region_name="eu-central-1",
                                                          aws_access_key_id=creds["AccessKeyId"],
                                                          aws_secret_access_key=creds["SecretAccessKey"],
                                                          aws_session_token=creds["SessionToken"])

        # Getting endpoint URL for LIST_FRAGMENTS
        get_endpoint_list_fragments_response = kinesis_client.get_data_endpoint(StreamName=stream_name,
                                                                                APIName="LIST_FRAGMENTS")

        # Getting endpoint URL for GET_MEDIA_FOR_FRAGMENT_LIST
        get_endpoint_get_media_response = kinesis_client.get_data_endpoint(StreamName=stream_name,
                                                                           APIName="GET_MEDIA_FOR_FRAGMENT_LIST")

        # Fetch DataEndpoint field
        list_fragments_url = get_endpoint_list_fragments_response["DataEndpoint"]
        get_media_url = get_endpoint_get_media_response["DataEndpoint"]

        ### List fragments step ###
        # Create Kinesis archive media client for list_fragments()
        list_fragments_client: KinesisVideoArchivedMediaClient = boto3.client(
            "kinesis-video-archived-media",
            endpoint_url=list_fragments_url,
            region_name="eu-central-1",
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"])

        # Get all fragments within timestamp range (start_time, end_time)
        list_fragments_response = list_fragments_client.list_fragments(
            StreamName=stream_name,
            MaxResults=1000,
            FragmentSelector={
                "FragmentSelectorType": selector,
                "TimestampRange": {
                    "StartTimestamp": start_time,
                    "EndTimestamp": end_time
                }
            }
        )

        _logger.debug("Got fragments for test clip on [%s]", stream_name)

        # Sort fragments by their timestamp and store start and end timestamps
        sorted_fragments = sorted(list_fragments_response["Fragments"], key=lambda d: datetime.timestamp(
            (d["ProducerTimestamp"])))
        fragments_start_time = sorted_fragments[0]["ProducerTimestamp"]
        last_fragment_length = timedelta(
            milliseconds=sorted_fragments[-1]["FragmentLengthInMilliseconds"])
        fragments_end_time = sorted_fragments[-1]["ProducerTimestamp"] + \
            last_fragment_length

        # Create comprehension list with sorted fragments
        fragment_numbers = [frag["FragmentNumber"]
                            for frag in sorted_fragments]

        ### Get media step ###
        # Create Kinesis archive media client for get_media_for_fragment_list()
        get_media_client: KinesisVideoArchivedMediaClient = boto3.client("kinesis-video-archived-media",
                                                                         endpoint_url=get_media_url,
                                                                         region_name="eu-central-1",
                                                                         aws_access_key_id=creds["AccessKeyId"],
                                                                         aws_secret_access_key=creds["SecretAccessKey"],
                                                                         aws_session_token=creds["SessionToken"])

        # Fetch all 2sec fragments from previously sorted list
        get_media_response = get_media_client.get_media_for_fragment_list(
            StreamName=stream_name,
            Fragments=fragment_numbers
        )

        _logger.debug("Got video file for test clip on [%s]", stream_name)

        # On completion of the context or destruction of the temporary directory object,
        # the newly created temporary directory and all its contents are removed from the filesystem.
        with tempfile.TemporaryDirectory() as auto_cleaned_up_dir:

            ### Conversion step (webm -> mp4) ###
            # Defining temporary files names
            input_name = os.path.join(auto_cleaned_up_dir, "received_file.webm")
            output_name = os.path.join(auto_cleaned_up_dir, "converted_file.mp4")

            # Read all bytes from http response body
            # (botocore.response.StreamingBody)
            video_chunk = get_media_response["Payload"].read()

            # Save video chunks to file
            with open(input_name, "wb") as infile:
                infile.write(video_chunk)

            # Convert .avi input file into .mp4 using ffmpeg
            _logger.debug("Starting ffmpeg conversion")
            process = subprocess.Popen(["/usr/bin/ffmpeg", "-i", input_name, "-qscale",  # nosec pylint: disable=consider-using-with
                                        "0", "-filter:v", "fps=15.72", output_name],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       universal_newlines=True)

            # Wait for completion
            _, _ = process.communicate()

            _logger.debug("Finished ffmpeg conversion")

            # Load bytes from converted output file
            with open(output_name, "rb") as output_file:
                output_video = output_file.read()

        # Generate processing end message
        _logger.info("Test clip download completed!")

        return output_video, fragments_start_time, fragments_end_time

    ##### Logs/Misc. functions ####################################################################
    @staticmethod
    def display_processed_msg(key_path, uid=None):
        """Displays status message for processing completion

        Arguments:
            key_path {string} -- [string containing the path + name of the file,
                                  whose processing status is being updated to
                                  completed (e.g. "uber/test_file_s3.txt")]
            uid {string} -- [string containing an identifier used only in the
                             processing containers to keep track of the
                             process of a given file. Optional]
        """
        _logger.info(
            "Processing of key [%s] complete! (uid: [%s])", key_path, uid)

    @staticmethod
    def list_s3_objects(s3_path: str, bucket: str, s3_client: S3Client,
                        delimiter: str = "", max_iterations: int = 1) -> ListObjectsV2OutputTypeDef:
        """
        List S3 objects and bypasses the 1000 object limitation by making multiple request.
        The number of maximum objects returned is equal to max_iterations*1000.

        Args:
            s3_path (str): path to S3 that should return a list of objects
            bucket (str): bucket name
            s3_client (S3Client) s3 client
            delimiter (str): delimiter to be applied
            max_iterations: number of maximum requests to make to list_objects_v2
        """
        if max_iterations < 1:
            raise ValueError("List_s3_objects needs do at least one iteration")

        continuation_token = ""  # nosec
        results: Optional[ListObjectsV2OutputTypeDef] = None

        for i in range(0, max_iterations):

            if i == 0:
                # First API call
                response_list = s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=s3_path,
                    Delimiter=delimiter
                )
                results = response_list

                # Make sure that the dictionary has a list for  keys and paths
                if "CommonPrefixes" not in results:
                    results["CommonPrefixes"] = []

                if "Contents" not in results:
                    results["Contents"] = []

            else:
                response_list = s3_client.list_objects_v2(
                    Bucket=bucket,
                    ContinuationToken=continuation_token,
                    Prefix=s3_path,
                    Delimiter=delimiter
                )

                # Append new file contents and paths
                if "Contents" in response_list:
                    results["Contents"].extend(response_list["Contents"])  # type: ignore # pylint: disable=E1136

                # Append CommonPrefixes if they exist
                if "CommonPrefixes" in response_list:
                    results["CommonPrefixes"].extend(  # type: ignore # pylint: disable=E1136
                        response_list["CommonPrefixes"])

                # Add key count
                results["KeyCount"] += response_list["KeyCount"]  # type: ignore # pylint: disable=E1137,E1136
                results["MaxKeys"] += results["MaxKeys"]  # type: ignore # pylint: disable=E1137,E1136

            # If all objects have been returned for the specific key/path, break
            if not response_list.get("IsTruncated", False):
                results["IsTruncated"] = False  # type: ignore # pylint: disable=E1137
                break

            # Set continuation token for next loop
            if "NextContinuationToken" not in response_list:
                results["IsTruncated"] = False  # type: ignore # pylint: disable=E1137
                break

            # Set continuation token and delete it from repsonse_list
            continuation_token = response_list["NextContinuationToken"]

        _logger.debug("Returning a total of %d s3 keys for %s in %s",
                      results["KeyCount"], bucket, s3_path)  # type: ignore

        # Clean variable fields
        results.pop("NextContinuationToken", None)  # type: ignore
        return results  # type: ignore

    @staticmethod
    def _check_if_deviceid_exists(s3_client: S3Client, s3_params: RCCS3ObjectParams) -> bool:
        """
        Checks if device exists in S3 bucket.

        Args:
            s3_client (S3Client): _description_
            s3_params (RCCS3ObjectParams): _description_

        Returns:
            bool: True if has permissions, False otherwise
        """
        try:
            prefix = f"{s3_params.tenant}/{s3_params.deviceid}/"
            # does the device exist?
            ContainerServices.list_s3_objects(
                prefix, s3_params.bucket, s3_client)

            return True
        except ClientError:  # pylint: disable=broad-except
            deviceid_error_message = f"""Could not access folder {s3_params.bucket}/{prefix} -
                                         Tenant {s3_params.tenant} is accessible,
                                         but could not access device {s3_params.deviceid}"""
            _logger.error(deviceid_error_message)

            return False

    def check_if_tenant_and_deviceid_exists_and_log_on_error(
            self,
            s3_client: S3Client,
            s3_object_params: RCCS3ObjectParams) -> bool:
        """Checks if tenant exists and also if deviceid exists to provide logging information if it doesn"t.

        Args:
            s3_client (S3Client): boto3 S3 client
            s3_object_params (RCCS3ObjectParams): wrapper for S3 information

        Returns:
            bool: True if exists, False otherwise
        """
        try:
            # can we access the tenant? (not by default, accessible tenants must be
            # whitelisted on RCC by RideCare Cloud Operations)
            prefix = f"{s3_object_params.tenant}/"
            ContainerServices.list_s3_objects(
                prefix, s3_object_params.bucket, s3_client)

            return ContainerServices._check_if_deviceid_exists(
                s3_client, s3_object_params)
        except ClientError:  # pylint: disable=broad-except
            tenant_error_message = f"""Could not access {s3_object_params.bucket}/{prefix} -
                                     our AWS IAM role is likely forbidden
                                     from accessing tenant {s3_object_params.tenant}"""
            _logger.error(tenant_error_message)
            return False

    @staticmethod
    def check_s3_file_exists(s3_client: S3Client, bucket: str, path: str) -> bool:
        """
        Get a file from an S3 bucket.
        If there is no file in the specific path it returns None.

        Args:
            s3_client (S3Client): S3 client to get file
            path (str): path to file
            bucket (str): s3 bucket client

        Returns:
            bool: Response boto3.get_object or None if the file does not exist
        """

        try:

            s3_client.head_object(
                Bucket=bucket,
                Key=path
            )

            return True
        except ClientError as excpt:
            # Following this: https://stackoverflow.com/questions/33068055/how-to-handle-errors-with-boto3

            if excpt.response["Error"]["Code"] == "404":
                return False

            _logger.error(
                "An error has ocurred while checking for the existance of %s key in %s bucket.",
                path,
                bucket)
            raise excpt
