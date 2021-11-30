"""Shared functions class for communication with AWS services"""
import json
import logging
import os
from datetime import datetime
import pytz
import boto3
from pymongo import MongoClient, errors


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
        self.__docdb_whitelist = {}
        self.__sdr_folder = ""
        self.__sdr_blacklist = {}

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

    @property
    def docdb_whitelist(self):
        """docdb_whitelist variable"""
        return self.__docdb_whitelist

    @property
    def sdr_folder(self):
        """sdr_folder variable"""
        return self.__sdr_folder

    @property
    def sdr_blacklist(self):
        """sdr_blacklist variable"""
        return self.__sdr_blacklist

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

        # List of all parameters whitelisted for docdb queries
        self.__docdb_whitelist = dict_body['docdb_key_whitelists']
        
        # Name of the Raw S3 bucket folder where to store RCC KVS clips
        self.__sdr_folder = dict_body['sdr_dest_folder']

        # Dictionary containing the tenant blacklists for processing and storage of RCC clips  
        self.__sdr_blacklist = dict_body['sdr_blacklist_tenants']

        logging.info("Load complete!\n")

    ##### SQS related functions #########################################################################
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

    ##### DB related functions #########################################################################
    @staticmethod
    def create_db_client():
        """TODO

        Returns:
            db {TODO} -- [TODO]
        """
        # Build connection info to access DocDB cluster
        docdb_info = {
            'cluster_endpoint': 'data-ingestion-cluster.cluster-czddtysxwqch.eu-central-1.docdb.amazonaws.com',
            'tls': 'true',
            'tlsCAFile': 'rds-combined-ca-bundle.pem',
            'replicaSet': 'rs0',
            'readPreference': 'secondaryPreferred',
            'retryWrites': 'false',
            'db': 'DB_data_ingestion'
            }
        
        #region_name = "eu-central-1"
        secret_name = "data-ingestion-cluster-credentials"

        # TODO: ADD docdb_info TO CONFIG S3 FILE!!

        # Create the necessary client for AWS secrets manager access
        secrets_client = boto3.client('secretsmanager',
                                    region_name='eu-central-1')

        # Get password and username from secrets manager
        response = secrets_client.get_secret_value(SecretId=secret_name)
        str_response = response['SecretString']

        # Converts response body from string to dict
        # (in order to perform index access)
        new_body = str_response.replace("\'", "\"")
        dict_response = json.loads(new_body)

        # Create a MongoDB client, open a connection to Amazon DocumentDB
        # as a replica set and specify the read preference as
        # secondary preferred
        client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=dict_response['username'],
                         password=dict_response['password'],
                         tls=docdb_info['tls'],
                         tlsCAFile=docdb_info['tlsCAFile'],
                         replicaSet=docdb_info['replicaSet'],
                         readPreference=docdb_info['readPreference'],
                         retryWrites=docdb_info['retryWrites']
                        )
        
        # Specify the database to be used
        db = client[docdb_info['db']]
    
        return db
    
    @staticmethod
    def generate_recording_item(data, table_rec, timestamp):
        """TODO

        Returns:
            db {TODO} -- [TODO]
        """
        # Build item structure and add info from msg received
        item_db = {
                    '_id': data["_id"],
                    's3_path': data["s3_path"],
                    'MDF_available': data["MDF_available"],
                    'recording_overview': data["recording_overview"],
                    'results_CHC': []
                }
        
        # Fetch metadata file if it exists
        if data["MDF_available"] == "Yes":

            # Create S3 client to download metadata
            s3_client = boto3.client('s3',
                        region_name='eu-central-1')

            s3_bucket, video_key = data["s3_path"].split("/", 1)
            s3_key = video_key.split(".")[0] + '_metadata_full.json'

            # Download metadata json file
            response = s3_client.get_object(
                                                Bucket=s3_bucket,
                                                Key=s3_key
                                            )

            # Decode and convert file contents into json format
            result_info = json.loads(response['Body'].read().decode("utf-8"))

            # Get info to populate CHC blocked events arrays (CHC)
            chb_array = []

            # Check all frames
            for frame in result_info['frame']:
                # Validate every frame (check if it has objectlist parameter)
                if 'objectlist' in frame.keys():
                    for item in frame['objectlist']:
                        # Check for item with ID = 1
                        # (it has the CameraViewBlocked info)
                        if item['id'] == '1':
                            chb_value = item['floatAttributes'][0]['value']
                            chb_array.append(chb_value)
                else:
                    chb_array.append("0")

            # Add array from metadata full file to created item
            mdf_data = {    
                        'algo_out_id': "-",
                        'source': "MDF",
                        'CHBs': chb_array,
                        'number_CHC_events': "",
                        'lengthCHC': ""
                    }

            item_db['results_CHC'].append(mdf_data)

        # Insert previous built item on the Recording collection
        table_rec.insert_one(item_db)

        # Create logs message
        logging.info("[%s]  Recording DB item (Id: %s) created!", timestamp, data["_id"])
        
    @staticmethod
    def update_pipeline_db(data, table_pipe, timestamp, unique_id, source, container_name):
        """TODO

        Returns:
            db {TODO} -- [TODO]
        """
        # Initialise variables used in both item creation and update
        status = data['data_status']

        # Check if item with that name already exists
        response = table_pipe.find_one({'_id': unique_id})

        if response:
            # Update the existing records
            table_pipe.update({'_id': unique_id}, {"$set": {"data_status": status, "info_source": source, "last_updated": timestamp}})

            # Create logs message
            logging.info("[%s]  Pipeline Exec DB item (Id: %s) updated!", timestamp, unique_id)
        
        else:
            # Build item structure and add info from msg received
            item_db = {
                        '_id': unique_id,
                        'data_status': status,
                        'from_container': container_name,
                        'info_source': source,
                        'last_updated': timestamp,
                        'processing_list': data['processing_steps'],
                        's3_path': data['s3_path']
                    }

            # Insert previous built item
            table_pipe.insert_one(item_db)

            # Create logs message
            logging.info("[%s]  Pipeline Exec DB item (Id: %s) created!", timestamp, unique_id)

    @staticmethod
    def update_outputs_db(data, table_algo_out, table_rec, timestamp, unique_id, source):
        """TODO

        Returns:
            db {TODO} -- [TODO]
        """
        # Initialise variables used in item creation
        outputs = data['output']
        run_id = unique_id + '_' + source

        # Item creation (common parameters)
        item_db = {
                    '_id': run_id,
                    'algorithm_id': source,
                    'pipeline_id': unique_id
                }

        # Populate output_paths parameter
        item_db['output_paths'] = {}

        # Check if there is a metadata file path available
        if outputs['meta_path'] == "-":
            item_db['output_paths']["metadata"] = "-"
        else:
            metadata_path = outputs['bucket'] + '/' + outputs['meta_path']
            item_db['output_paths']["metadata"] = metadata_path

        # Check if there is a video file path available
        if outputs['video_path'] == "-":
            item_db['output_paths']["video"] = "-"
        else:
            video_path = outputs['bucket'] + '/' + outputs['video_path']
            item_db['output_paths']["video"] = video_path

        # Compute results from CHC processing
        if source == "CHC":
            # Build default results structure
            item_db['results'] = {
                                    'CHBs': [],
                                    'number_CHC_events': "",
                                    'lengthCHC': ""
                                }

            # Create S3 client to download metadata
            s3_client = boto3.client('s3',
                        region_name='eu-central-1')

            # Download metadata json file
            response = s3_client.get_object(
                Bucket=outputs['bucket'],
                Key=outputs['meta_path']
            )

            # Decode and convert file contents into json format
            result_info = json.loads(response['Body'].read().decode("utf-8"))

            # Get info to populate CHC blocked events arrays (CHC)
            chb_array = []

            # Check all frames
            for frame in result_info['frame']:
                # Validate every frame (check if it has objectlist parameter)
                if 'objectlist' in frame.keys():
                    for item in frame['objectlist']:
                        # Check for item with ID = 1
                        # (it has the CameraViewBlocked info)
                        if item['id'] == '1':
                            chb_value = item['floatAttributes'][0]['value']
                            chb_array.append(chb_value)
                else:
                    chb_array.append("0")

            # Add array from ivs_chain metadata file to created item
            item_db['results']['CHBs'] = chb_array

            # Add array from CHC output file to recording DB item
            chc_data = {    
                        'algo_out_id': run_id,
                        'source': "CHC",
                        'CHBs': chb_array,
                        'number_CHC_events': "",
                        'lengthCHC': ""
                    }
            try:
                # Update recording DB item (appends chc_data to results list)
                table_rec.update({'_id': unique_id}, {'$push': {'results_CHC': chc_data}})

                # Create logs message
                logging.info("[%s]  Recording DB item (Id: %s) updated!", timestamp, unique_id)

            except Exception as e:
                logging.info(e)
                logging.info(chc_data)
            
        try:
            # Insert previous built item
            table_algo_out.insert_one(item_db)

        except errors.DuplicateKeyError as e:
            # Raise error exception if duplicated item is found
            # NOTE: In this case, the old item is not overriden!
            logging.info(e)
            logging.info(item_db)

        logging.info("[%s]  Algo Output DB item (run_id: %s) created!", timestamp, run_id)
    
    def connect_to_docdb(self, data, attributes):
        """TODO

        Arguments:
            data {TODO} -- []
            attributes {TODO} -- []
        """
        # Create DocDB client
        db = self.create_db_client()

        # Specify the tables to be used
        table_pipe = db[self.__db_tables['pipeline_exec']]
        table_algo_out = db[self.__db_tables['algo_output']]
        table_rec = db[self.__db_tables['recording']]

        # Create timestamp for the current time
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))

        # Get source container name
        source = attributes['SourceContainer']['StringValue']

        #################### NOTE: Recording collection handling ##################

        # SDRetriever container info is processed in a different
        # way from the other containers
        if source == "SDRetriever":
            # Call respective processing function
            self.generate_recording_item(data, table_rec, timestamp)
            return

        #################### NOTE: Pipeline execution collection handling ####################
        
        # Get filename (id) from message received
        # Note: If source container is SDRetriever then:
        #            data["s3_path"] = bucket + key
        #       Otherwise, for all other containers:
        #            data["s3_path"] = key
        #
        unique_id = os.path.basename(data["s3_path"]).split(".")[0]

        # Call respective processing function
        self.update_pipeline_db(data,
                                table_pipe,
                                timestamp,
                                unique_id,
                                source,
                                self.__container['name'])


        ###################################################### DEBUG MANUAL UPLOADS (TODO: REMOVE AFTERWARDS)
        #####################################################################################################
        # Check if item with that name already exists
        response_rec = table_rec.find_one({'_id': unique_id})
        if response_rec:
            pass
        else:
            # Create empty item for the recording
            item_db = {
                       '_id': unique_id,
                       's3_path': self.__s3_buckets['raw'] + "/" + data["s3_path"],
                       'MDF_available': "No",
                       'recording_overview':{
                                             "length": "",
                                             "time": "",
                                             "deviceID": "",
                                             "resolution": "",
                                             "#snapshots": "",
                                             "snapshots_paths": {}
                                            },
                        "results_CHC": []
                }
            # Insert previous built item on the Recording collection
            table_rec.insert_one(item_db)
            # Create logs message
            logging.info("[%s]  Recording DB empty item (Id: %s) created!", timestamp, unique_id)
        #####################################################################################################
        #####################################################################################################


        #################### NOTE: Algorithm output collection handling ####################
        # Create/Update item on Algorithm Output DB
        if 'output' in data:
            # Call respective processing function
            self.update_outputs_db(data,
                                   table_algo_out,
                                   table_rec,
                                   timestamp,
                                   unique_id,
                                   source)

    ##### S3 related functions #########################################################################
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

        # TODO: ADD THIS INFO TO CONFIG FILE
        type_dict = {
                      "json": "application/json",
                      "mp4": "video/mp4",
                      "avi": "video/x-msvideo",
                      "txt": "text/plain"
                    }
        file_extension = key_path.split('.')[-1]
         
        response = client.put_object(
                                        Body=object_body,
                                        Bucket=s3_bucket,
                                        Key=key_path,
                                        ServerSideEncryption='aws:kms',
                                        ContentType=type_dict[file_extension]
                                    )

        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Upload completed!", timestamp)

    ##### Kinesis related functions ####################################################################
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
            video_clip {bytes} -- [Received chunk in bytes format]
        """
        timestamp = str(datetime.now(tz=pytz.UTC).strftime(self.__time_format))
        logging.info("[%s]  Downloading clip (stream: %s)..", timestamp,
                                                              stream_name)

        # Create a kinesis client with temporary STS credentials
        # to enable cross-account access
        kinesis_client = boto3.client('kinesisvideo',
                                      region_name='eu-central-1',
                                      aws_access_key_id=creds['AccessKeyId'],
                                      aws_secret_access_key=creds['SecretAccessKey'],
                                      aws_session_token=creds['SessionToken'])

        # Getting endpoint URL for GET_CLIP
        response = kinesis_client.get_data_endpoint(StreamName=stream_name,
                                                    APIName='GET_CLIP')

        endpoint_response = response['DataEndpoint']

        # Create client using received endpoint URL
        media_client = boto3.client('kinesis-video-archived-media',
                                    endpoint_url=endpoint_response,
                                    region_name='eu-central-1',                                      
                                    aws_access_key_id=creds['AccessKeyId'],
                                    aws_secret_access_key=creds['SecretAccessKey'],
                                    aws_session_token=creds['SessionToken'])

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

    ##### Logs/Misc. functions #########################################################################
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
