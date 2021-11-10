"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
from datetime import datetime

CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v3.0 (S3 Test)"      # Version of the current container


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
    """
    input_sqs = container_services.input_queue
    logging.info("Processing %s SQS message (Kinesis)..\n", input_sqs)

    # Converts message body from string to dict
    # (in order to perform index access)
    new_msg = message['Body'].replace("\'", "\"")
    dict_msg = json.loads(new_msg)

    # Converts value on Message parameter (where msg info is stored) 
    # also from string to dict (in order to perform index access)
    dict_body = json.loads(dict_msg['Message'])

    # Info from received message
    stream_name = dict_body['streamName']

    epoch_from = dict_body['from']
    start_time = datetime.fromtimestamp(epoch_from/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    epoch_to = dict_body['to']
    end_time = datetime.fromtimestamp(epoch_to/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    selector = 'PRODUCER_TIMESTAMP'
    stream_role = "arn:aws:iam::213279581081:role/dev-DevCloud"
    clip_ext = ".mp4"
    sts_session = "AssumeRoleSession1"
    
    #s3_folder = 'Debug_Lync/'
    s3_folder = container_services.sdr_folder

    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)

    if stream_name == "TEST_TENANT_INTEGRATION_TEST_DEVICE_InteriorRecorder":
        s3_path = s3_filename + clip_ext
    else:
        s3_path = s3_folder + s3_filename + clip_ext
    ####################################################################################################################

    # Requests credentials to assume specific cross-account role
    assumed_role_object = sts_client.assume_role(RoleArn=stream_role,
                                                 RoleSessionName=sts_session)

    role_credentials = assumed_role_object['Credentials']

    # Get Kinesis clip using received message parameters
    video_clip = container_services.get_kinesis_clip(role_credentials,
                                                     stream_name,
                                                     start_time,
                                                     end_time,
                                                     selector)

    # Upload video clip into raw data S3 bucket
    container_services.upload_file(s3_client,
                                   video_clip,
                                   container_services.raw_s3,
                                   s3_path)

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
    # Converts value on Message parameter (where msg info is stored) 
    # also from string to dict (in order to perform index access)
    dict_body = json.loads(dict_msg['Message'])

    #################################################################################################################################################
    # TODO: CONVERT MSG PARAMETERS TO BE USED ON THIS FUNCTION

    # TEST VALUES
    bucket_origin = 'rcc-dev-device-data'
    key_prefix = "InteriorRecorder_InteriorRecorder-768bf358-24dc-495e-a63e-aad1d3ce1bb7"

    # name of the folder and file for the final concatenated file
    #key_full_metadata = 'Debug_Lync/InteriorRecorder_InteriorRecorder-768bf358-24dc-495e-a63e-aad1d3ce1bb7_metadata_full.json'

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    s3_role = "arn:aws:iam::213279581081:role/dev-DevCloud"
    sts_session = "AssumeRoleSession2"

    # Info from received message
    stream_name = dict_body['streamName']
    epoch_from = dict_body['from']
    epoch_to = dict_body['to']

    #s3_folder = 'Debug_Lync/'
    s3_folder = container_services.sdr_folder
    s3_file_extension = '_metadata_full.json'
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)

    if stream_name == "TEST_TENANT_INTEGRATION_TEST_DEVICE_InteriorRecorder":
        key_full_metadata = s3_filename + s3_file_extension
    else:
        key_full_metadata = s3_folder + s3_filename + s3_file_extension
    #################################################################################################################################################

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

    # Get list of all files with the same key prefix as the one
    # received on the message
    response_list = rcc_s3.list_objects_v2(
        Bucket=bucket_origin,
        Prefix=key_prefix
    )               
    
    #################### UNCOMMENT THIS PART TO ENABLE CONCATENATION
    # Initialise dictionary that will store all files
    # that match the received prefix
    files_dict = {}

    # Cycle through the received list of matching files,
    # download them from S3 and store them on the files_dict dictionary
    for index, file_entry in enumerate(response_list['Contents']):

        metadata_file = container_services.download_file(rcc_s3,
                                                         bucket_origin,
                                                         file_entry['Key'])

        # Read all bytes from http response body
        # (botocore.response.StreamingBody) and convert them into json format
        json_temp = json.loads(metadata_file.decode("utf-8"))

        # Store json file on the dictionary based on the index
        files_dict[index] = json_temp

    # Define total number of metadata_full files received
    chunks_total = len(files_dict)

    # Initialise dictionary that will comprise all concatenated info
    final_dict = {}

    # Use the resolution of the first file (assumption: resolution is the same
    # for all received files)
    final_dict['resolution'] = files_dict[0]['resolution']

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
    
    # Upload final concatenated file
    container_services.upload_file(s3_client,
                                    concatenated_file,
                                    container_services.raw_s3,
                                    key_full_metadata)

    #####################################
    '''
    # CROSS ACCOUNT S3 ACCESS TEST
    for index, file_entry in enumerate(response_list['Contents']):

        #if file_entry['Key'].endswith('.json'):

        file_key = file_entry['Key'].split("/")[-1]
        #logging.info("%s\n", file_key)
        logging.info("\n")
        
        metadata_file = container_services.download_file(rcc_s3,
                                                        bucket_origin,
                                                        file_entry['Key'])

        key_full_metadata = 'uber/' + file_key

        container_services.upload_file(s3_client,
                                        metadata_file,
                                        container_services.raw_s3,
                                        key_full_metadata)
    '''
    ######################################


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
            transfer_kinesis_clip(s3_client,
                                  sts_client,
                                  container_services,
                                  message)

            # Concatenate all metadata related to processed clip
            concatenate_metadata_full(s3_client,
                                      sts_client,
                                      container_services,
                                      message)

            # Delete message after processing
            container_services.delete_message(sqs_client,
                                              message['ReceiptHandle'])


if __name__ == '__main__':
    main()
