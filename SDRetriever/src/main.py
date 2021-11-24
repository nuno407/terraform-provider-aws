"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
from datetime import timedelta as td, datetime
import pytz
import subprocess

CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v4.0"      # Version of the current container


def transfer_kinesis_clip(s3_client, sts_client, container_services, message):
    """Converts the message body to json format (for easier variable access),
    gets video clip from RCC Kinesis video stream and stores the received
    clip on the raw data S3 bucket to be later processed by the data
    ingestion pipeline.

    Arguments:
        s3_client {boto3.client} -- [client used to access
                                     the S3 service]
        sts_client {boto3.client} -- [client used to assume
                                      a given cross-account IAM role]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        message {dict} -- [dict with the received message content
                           (for more info please check the response syntax
                           of the Boto3 SQS.client.receive_message method)]
    Returns:
        record_data {dict} -- [TODO]
    """
    input_sqs = container_services.input_queue
    logging.info("Processing %s SQS message (Kinesis)..\n", input_sqs)

    ###########################################
    #DEBUG
    logging.info(message)
    ##########################################

    # Converts message body from string to dict
    # (in order to perform index access)
    new_msg = message['Body'].replace("\'", "\"")
    dict_msg = json.loads(new_msg)

    # Converts value on Message parameter (where msg info is stored) 
    # also from string to dict (in order to perform index access)
    dict_body = json.loads(dict_msg['Message'])

    # Get device ID from message attributes
    dict_attr = dict_msg['MessageAttributes']
    device = dict_attr['deviceId']

    record_data = {}

    try:
        # Info from received message
        stream_name = dict_body['streamName']

        # epoch_from = dict_body['from']
        # start_time = datetime.fromtimestamp(epoch_from/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        # epoch_to = dict_body['to']
        # end_time = datetime.fromtimestamp(epoch_to/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        ##### New implementation for SNS changes #######################################################################################################################
        epoch_from = dict_body['footageFrom']
        start_time = datetime.fromtimestamp(epoch_from/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        epoch_to = dict_body['footageTo']
        end_time = datetime.fromtimestamp(epoch_to/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        ############################################################################################################################

    except Exception as e:
        logging.info("\nWARNING: Message (id: %s) contains unsupported info! Please check the error below:", message['MessageId'])
        logging.info(e)
        logging.info("\n")
        return record_data

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    selector = 'PRODUCER_TIMESTAMP'
    stream_role = "arn:aws:iam::213279581081:role/dev-DevCloud"
    clip_ext = ".mp4"
    sts_session = "AssumeRoleSession1"
    
    # Defining s3 path to store KVS clip
    s3_folder = container_services.sdr_folder
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    s3_path = s3_folder + s3_filename + clip_ext

    # Check if there is a file with the same name already
    # stored on the S3 bucket (to avoid multiple processing of a given
    # video due to receiving duplicated SNS messages)
    response_list = s3_client.list_objects_v2(
        Bucket=container_services.raw_s3,
        Prefix=s3_path
    )         

    # Skips processing if video file is a duplicate
    if response_list['KeyCount'] > 0:
        logging.info("\nWARNING: Recording (path: %s) already exists on the S3 bucket and will be skipped!!\n", s3_path)
        return record_data

    # Requests credentials to assume specific cross-account role
    assumed_role_object = sts_client.assume_role(RoleArn=stream_role,
                                                 RoleSessionName=sts_session)

    role_credentials = assumed_role_object['Credentials']

    try:
        # Get Kinesis clip using received message parameters
        video_clip = container_services.get_kinesis_clip(role_credentials,
                                                         stream_name,
                                                         start_time,
                                                         end_time,
                                                         selector)
    except Exception as e:
        logging.info("\nWARNING: Failed to get kinesis clip (%s)!!\n", s3_path)
        logging.info("\nReason: %s\n", e)
        return record_data

    # Upload video clip into raw data S3 bucket
    container_services.upload_file(s3_client,
                                   video_clip,
                                   container_services.raw_s3,
                                   s3_path)

    ################ NOTE: Extract info details from video ###############

    # Create timestamp for the current time
    timestamp = str(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S"))

    # Store input video file into current working directory
    input_name = "input_video.mp4"
    with open(input_name, "wb") as input_file:
        input_file.write(video_clip)

    # Execute ffprobe command to get video clip info
    result = subprocess.run(["ffprobe", "-v", "error", "-show_format",
                             "-show_streams", "-print_format", "json",
                             input_name],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)

    # Convert bytes from ffprobe output to string
    decoded_info = (result.stdout).decode("utf-8")

    # Convert info string to json
    video_info = json.loads(decoded_info)

    # Get video resolution
    width = str(video_info["streams"][0]["width"])
    height = str(video_info["streams"][0]["height"])
    video_resolution = width + "x "+ height

    # Get video duration
    video_seconds = round(float(video_info["format"]["duration"]))
    video_duration = str(td(seconds=video_seconds))

    # Build dictionary with info to store on DB (Recording collection)
    record_data["_id"] = s3_filename
    record_data["recording_overview"] = {}
    record_data["recording_overview"]["length"] = video_duration
    record_data["recording_overview"]["time"] = timestamp
    record_data["recording_overview"]["deviceID"] = device
    record_data["recording_overview"]["resolution"] = video_resolution
    record_data["recording_overview"]["#snapshots"] = "0"
    record_data["recording_overview"]["snapshots_paths"] = {}

    return record_data

def concatenate_metadata_full(s3_client, sts_client, container_services, message):
    """Converts the message body to json format (for easier variable access),
    gets all metadata_full json files from RCC S3 bucket related to
    the previous processed video clip, concatenates all the info
    and stores the resulting json file on the raw data S3 bucket.

    Arguments:
        s3_client {boto3.client} -- [client used to access
                                     the S3 service]
        sts_client {boto3.client} -- [client used to assume
                                      a given cross-account IAM role]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
        message {dict} -- [dict with the received message content
                           (for more info please check the response syntax
                           of the Boto3 SQS.client.receive_message method)]
    """
    input_sqs = container_services.input_queue
    logging.info("\nProcessing %s SQS message (Concatenation)..\n", input_sqs)

    # Converts message body from string to dict
    # (in order to perform index access)
    new_msg = message['Body'].replace("\'", "\"")
    dict_msg = json.loads(new_msg)

    # Converts value on Message and MessageAttributes parameters
    # (where msg info is stored) also from string to dict
    # (in order to perform index access)
    dict_body = json.loads(dict_msg['Message'])
    #dict_attr = json.loads(dict_msg['MessageAttributes'])
    dict_attr = dict_msg['MessageAttributes']

    # Define metadata files S3 bucket location (RCC)
    # NOTE: This bucket name will always be the same
    #       (confirmed by the HoneyBadgers team)
    bucket_origin = 'rcc-dev-device-data'

    #################################################################################

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    s3_role = "arn:aws:iam::213279581081:role/dev-DevCloud"
    sts_session = "AssumeRoleSession2"

    # Requests credentials to assume specific cross-account role
    assumed_role_object = sts_client.assume_role(RoleArn=s3_role,
                                                 RoleSessionName=sts_session)

    role_creds = assumed_role_object['Credentials']

    # Create a S3 client with temporary STS credentials
    # to enable cross-account access
    rcc_s3 = boto3.client('s3',
                          region_name='eu-central-1',
                          aws_access_key_id=role_creds['AccessKeyId'],
                          aws_secret_access_key=role_creds['SecretAccessKey'],
                          aws_session_token=role_creds['SessionToken'])

    #################################################################################

    # TEST VALUES
    # key_prefix = "honeybadger/ivs_srx_develop_tmk2si_01/year=2021/month=11/day=04/hour=08/InteriorRecorder_InteriorRecorder-768bf358-24dc-495e-a63e-aad1d3ce1bb7"

    # # name of the folder and file for the final concatenated file
    # #key_full_metadata = 'Debug_Lync/InteriorRecorder_InteriorRecorder-768bf358-24dc-495e-a63e-aad1d3ce1bb7_metadata_full.json'

    # # Info from received message
    # stream_name = dict_body['streamName']
    # epoch_from = dict_body['from']
    # epoch_to = dict_body['to']

    ##### New implementation for SNS changes #######################################################################################################################
    # Info from received message (MessageAttributes parameter)
    rec_prefix = dict_attr['recordingId']
    device = dict_attr['deviceId']
    tenant = dict_attr['tenant']
    recorder = dict_attr['recorder']
    
    # Info from received message (Message parameter)
    stream_name = dict_body['streamName']
    epoch_from = dict_body['footageFrom']
    epoch_to = dict_body['footageTo']
    upload_start = dict_body['uploadStarted']
    upload_end = dict_body['uploadFinished']

    # Convert start timestamp (Metadata) to datetime
    meta_start_time = datetime.fromtimestamp(upload_start/1000.0)
    # Round down to exact hour (i.e. 0min 0s)
    round_start_time = meta_start_time.replace(microsecond=0, second=0, minute=0)

    # Convert end timestamp (Metadata) to datetime
    meta_end_time = datetime.fromtimestamp(upload_end/1000.0)
    # Round down to exact hour (i.e. 0min 0s)
    round_end_time = meta_end_time.replace(microsecond=0, second=0, minute=0)

    # Calculate delta between start and end timestamps
    delta = round_end_time-round_start_time
    
    # Convert delta from seconds to hours
    hours_conv = divmod(delta.seconds, 3600.0)[0]

    # Round up previous result and add 24h for each day (if any)
    # present on the delta result
    delta_hours = round(hours_conv) + delta.days*24 + 1    

    # Initialise dictionary that will store all files
    # that match the received prefix
    files_dict = {}

    # Create counter for indexing and to get total number
    # of metadata_full files received
    chunks_total = 0

    # Generate a timestamp path for each hour within the calculated delta
    # and get all files that match the key prefix
    for temp_hour in range(int(delta_hours)):

        # Increment the start timestamp by the number of hours
        # defined in temp_hour to generate the next timestamp path
        next_time = round_start_time + td(hours=temp_hour)

        # Construct timestamp part of the s3 path (folder)
        time_path = "year={}/month={}/day={}/hour={}".format(next_time.year,
                                                             next_time.month,
                                                             next_time.day,
                                                             next_time.hour)
        
        # Build s3 key prefix
        key_prefix = tenant + "/" + device + "/" + time_path + "/" + recorder + "_" + rec_prefix

        # Get list of all files with the same key prefix as the one
        # received on the message
        response_list = rcc_s3.list_objects_v2(
            Bucket=bucket_origin,
            Prefix=key_prefix
        )               
    
        # Check if response_list is not empty
        if response_list['KeyCount'] == 0:
            logging.info("\nWARNING: No metadata files with prefix: %s were found!!\n", key_prefix)
            return

        # Cycle through the received list of matching files,
        # download them from S3 and store them on the files_dict dictionary
        for index, file_entry in enumerate(response_list['Contents']):
            
            # Process only json files
            if file_entry['Key'].endswith('.json'):

                # Download metadata file from RCC S3 bucket
                metadata_file = container_services.download_file(rcc_s3,
                                                                bucket_origin,
                                                                file_entry['Key'])

                # Read all bytes from http response body
                # (botocore.response.StreamingBody) and convert them into json format
                json_temp = json.loads(metadata_file.decode("utf-8"))

                # Store json file on the dictionary based on the index
                files_dict[chunks_total] = json_temp

                # Increase counter for number of files received
                chunks_total += 1
    
    #################################################################################################################################################

    # # Get list of all files with the same key prefix as the one
    # # received on the message
    # response_list = rcc_s3.list_objects_v2(
    #     Bucket=bucket_origin,
    #     Prefix=key_prefix
    # )               
    
    # # Check if response_list is not empty
    # if response_list['KeyCount'] == 0:
    #     logging.info("\nWARNING: No metadata files with prefix: %s were found!!\n", key_prefix)
    #     return

    # # Initialise dictionary that will store all files
    # # that match the received prefix
    # files_dict = {}

    # # Create counter for indexing and to get total number
    # # of metadata_full files received
    # chunks_total = 0

    # # Cycle through the received list of matching files,
    # # download them from S3 and store them on the files_dict dictionary
    # for index, file_entry in enumerate(response_list['Contents']):
        
    #     # Process only json files
    #     if file_entry['Key'].endswith('.json'):

    #         # Download metadata file from RCC S3 bucket
    #         metadata_file = container_services.download_file(rcc_s3,
    #                                                         bucket_origin,
    #                                                         file_entry['Key'])

    #         # Read all bytes from http response body
    #         # (botocore.response.StreamingBody) and convert them into json format
    #         json_temp = json.loads(metadata_file.decode("utf-8"))

    #         # Store json file on the dictionary based on the index
    #         files_dict[chunks_total] = json_temp

    #         # Increase counter for number of files received
    #         chunks_total += 1

    #################################################################################

    # Initialise dictionary that will comprise all concatenated info
    final_dict = {}

    # Use the resolution of the first file
    # NOTE: assumption -> resolution is the same for all received files
    final_dict['resolution'] = files_dict[0]['resolution']


    #######################################################################
    # NOTE: assumption -> FIRST FILE HAS THE FIRST FRAME
    #######################################################################


    # Define chunk start point as the start from the first file and
    # end point as the end of the last file
    final_dict['chunk'] = {
                            "pts_start": files_dict[0]['chunk']['pts_start'],
                            "pts_end": files_dict[chunks_total-1]['chunk']['pts_end']
                        }

    # Frames Concatenation Process
    final_dict['frame'] = []
    for m in files_dict:
        for current_frame in files_dict[m]['frame']:
            final_dict['frame'].append(current_frame)

    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (bytes(json.dumps(final_dict, ensure_ascii=False, indent=4).encode('UTF-8')))
    
    #################################################################################

    # Defining s3 path to store concatenated metadata full json
    s3_folder = container_services.sdr_folder
    s3_file_extension = '_metadata_full.json'
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    key_full_metadata = s3_folder + s3_filename + s3_file_extension

    # Upload final concatenated file
    container_services.upload_file(s3_client,
                                    concatenated_file,
                                    container_services.raw_s3,
                                    key_full_metadata)


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
    sts_client = boto3.client('sts',
                              region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    logging.info("\nListening to input queue(s)..\n")

    # Main loop
    while(True):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            # Get and store kinesis video clip
            rec_data = transfer_kinesis_clip(s3_client,
                                             sts_client,
                                             container_services,
                                             message)

            # Checks if recording received is valid
            if rec_data:
                # Concatenate all metadata related to processed clip
                concatenate_metadata_full(s3_client,
                                          sts_client,
                                          container_services,
                                          message)

                # Send message to input queue of metadata container
                metadata_queue = container_services.sqs_queues_list["Metadata"]
                container_services.send_message(sqs_client,
                                                metadata_queue,
                                                rec_data)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])


if __name__ == '__main__':
    main()
