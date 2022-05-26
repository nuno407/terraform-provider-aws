"""Anonymize container script"""
import json
import logging
from datetime import timedelta as td, datetime
import subprocess
from operator import itemgetter
import os
import time
import boto3
from typing import TypeVar
from baseaws.shared_functions import ContainerServices
CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v5.2"      # Version of the current container
MAX_CONSECUTIVE = 25
"""This version added snapshot support."""
ST = TypeVar('ST', datetime, str, int)  # SnapshotTimestamp type


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
        record_data {dict} -- [If the kinesis clip associated to the
                                recording was successfully retrieved
                                from RCC, processed and stored on a S3
                                bucket, this variable returns a set of
                                data associated to that recording (i.e.
                                filename, S3 path and video metadata),
                                otherwise (i.e. if the process failed
                                at some point), it is returned {} (empty
                                dictionary)]
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

    # Get device ID from message attributes
    dict_attr = dict_msg['MessageAttributes']
    device = dict_attr['deviceId']['Value']

    # Build dictionary with info to store on DB (Recording collection)
    record_data = {}    
    hq_request = {}

    ###########################################
    # DEBUG
    tenant = dict_attr['tenant']['Value']
    if tenant == "TEST_TENANT":
        logging.info("WARNING: Message skipped (TEST_TENANT | %s)",
                     dict_body['streamName'])
        return record_data, hq_request

    #logging.info(message)
    ##########################################

    record_data["recording_overview"] = {}
    record_data["recording_overview"]['deviceID'] = device

    try:
        # Info from received message
        stream_name = dict_body['streamName']

        epoch_from = dict_body['footageFrom']
        start_time = datetime.fromtimestamp(
            epoch_from/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        epoch_to = dict_body['footageTo']
        end_time = datetime.fromtimestamp(
            epoch_to/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        record_data["recording_overview"]['time'] = str(start_time)

    except Exception:
        logging.info(
            "\n######################## Exception #########################")
        logging.exception(
            "ERROR: Message (id: %s) contains unsupported info!", message['MessageId'])
        logging.info(
            "############################################################\n")
        return record_data, hq_request

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    selector = 'PRODUCER_TIMESTAMP'
    # "arn:aws:iam::213279581081:role/dev-DevCloud"
    stream_role = container_services.rcc_info["role"]
    clip_ext = ".mp4"
    sts_session = "AssumeRoleSession1"

    # Defining s3 path to store KVS clip
    if "srxdriverpr" in stream_name:
        s3_folder = container_services.sdr_folder['driver_pr']
    else:
        s3_folder = container_services.sdr_folder['debug']

    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    s3_path = s3_folder + s3_filename + clip_ext
    record_data["_id"] = s3_filename
    record_data["s3_path"] = container_services.raw_s3 + "/" + s3_path

    # Check if there is a file with the same name already
    # stored on the S3 bucket (to avoid multiple processing of a given
    # video due to receiving duplicated SNS messages)
    response_list = s3_client.list_objects_v2(
        Bucket=container_services.raw_s3,
        Prefix=s3_path
    )

    # Skips processing if video file is a duplicate
    if response_list['KeyCount'] > 0:
        logging.info(
            "\nWARNING: Recording (path: %s) already exists on the S3 bucket and thus the download will be skipped!!\n", s3_path)
        return record_data, hq_request

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
        # Upload video clip into raw data S3 bucket
        container_services.upload_file(s3_client,
                                       video_clip,
                                       container_services.raw_s3,
                                       s3_path)
    except Exception:
        logging.info(
            "\n######################## Exception #########################")
        logging.exception("ERROR: Failed to get kinesis clip (%s)!!", s3_path)
        logging.info(
            "############################################################\n")
        return record_data, hq_request

    ################ NOTE: Extract info details from video ###############

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
    video_resolution = width + "x" + height

    # Get video duration
    video_seconds = round(float(video_info["format"]["duration"]))
    video_duration = str(td(seconds=video_seconds))

    # fill the dictionary
    record_data["recording_overview"]['length'] = video_duration
    record_data["recording_overview"]['resolution'] = video_resolution
    record_data["recording_overview"]['#snapshots'] = "0"
    record_data["recording_overview"]['snapshots_paths'] = {
    }

    # Generate dictionary with info to send to Selector container for
    # HQ data request
    if 'TrainingRecorder' not in stream_name:
        hq_request = {
            "streamName": stream_name,
            "deviceId": device,
            "footageFrom": epoch_from,
            "footageTo": epoch_to
        }

    return record_data, hq_request

def generate_compact_mdf_metadata(container_services, s3_client, epoch_from, epoch_to, files_dict, stream_name):
    """TODO

    Arguments:
        TODO
    """
    compact_frames = []

    final_info = {
        "partial_timestamps": {},
        "frames": [],
        "video": {
            "start": epoch_from,
            "end": epoch_to
        }
    }

    for partial_mdf in files_dict:

        pts_frames_list = []

        # Partial timestamps  ###############################################
        first_split = files_dict[partial_mdf]["filename"].split("._stream2_")
        second_split = first_split[1].split("_")
        human_timestamp = second_split[0]

        # Convert into datetime and then into epoch timestamp
        datetime_object = datetime.strptime(human_timestamp, "%Y%m%d%H%M%S")
        chunk_epoch_start = int(datetime_object.timestamp())

        # Frames  ###########################################################
        for pts_frame in files_dict[partial_mdf]["frame"]:

            pts_frames_list.append(int(pts_frame["number"]))

            frame_data = {
                "number": int(pts_frame["number"]),
                "timestamp": int(pts_frame["timestamp"]),
                "signals": {}
            }

            if 'objectlist' in pts_frame.keys():
                for item in pts_frame['objectlist']:
                    if 'boolAttributes' in item:
                        for attribute in item['boolAttributes']:
                            frame_data['signals'][attribute['name']] = (
                                attribute['value'] == 'true')
                    if 'floatAttributes' in item:
                        for attribute in item['floatAttributes']:
                            frame_data['signals'][attribute['name']
                                                  ] = float(attribute['value'])
                    if 'integerAttributes' in item:
                        for attribute in item['integerAttributes']:
                            frame_data['signals'][attribute['name']] = int(
                                attribute['value'])

            if len(frame_data['signals']) > 0:
                compact_frames.append(frame_data)

        # Partial timestamps full info #########################################
        final_info["partial_timestamps"][human_timestamp] = {
            "filename": files_dict[partial_mdf]["filename"],
            "pts_start": int(files_dict[partial_mdf]["chunk"]["pts_start"]),
            "converted_time": chunk_epoch_start,
            "frames_list": pts_frames_list
        }

    final_info["frames"] = sorted(
        compact_frames, key=lambda x: int(itemgetter("number")(x)))

    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (
        bytes(json.dumps(final_info, ensure_ascii=False, indent=4).encode('UTF-8')))

    # Defining s3 path to store concatenated metadata full json
    if "srxdriverpr" in stream_name:
        s3_folder = container_services.sdr_folder['driver_pr']
    else:
        s3_folder = container_services.sdr_folder['debug']

    s3_file_extension = '_compact_mdf.json'
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    key_full_metadata = s3_folder + s3_filename + s3_file_extension

    # Upload final concatenated file
    container_services.upload_file(s3_client,
                                   concatenated_file,
                                   container_services.raw_s3,
                                   key_full_metadata)

    return final_info

def generate_sync_data(container_services, s3_client, epoch_from, epoch_to, data_json, stream_name):
    """_summary_

    Args:
        container_services (_type_): _description_
        s3_client (_type_): _description_
        epoch_from (_type_): _description_
        epoch_to (_type_): _description_
        data_json (_type_): _description_
        stream_name (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Collect frame data and store it in separate dictionaries
    frame_ts = {}
    frame_camera_view = {}

    # Collect values for each frame
    for frame in data_json['frames']:
        # Collect relative timestamp for each frame
        frame_ts[frame['number']] = frame['timestamp']

        # Collect signals from key 'signals'
        frame_camera_view[frame['number']] = frame['signals']

    ###############################
    # Convert frame relative timestamps to real epoch timestamp
    real_frame_ts = {}

    for item in data_json['partial_timestamps']:
        # Collect reference relative timestamp for the current chunk
        pts_ref = data_json['partial_timestamps'][item]["pts_start"]
        # Collect reference real timestamp for the current chunk
        conv_ref = data_json['partial_timestamps'][item]["converted_time"]

        for frame in data_json['partial_timestamps'][item]["frames_list"]:
            if frame in frame_ts.keys():
                # Calculate time difference between chunk reference and current frame (relative ts)
                diff_ref_to_frame = (frame_ts[frame] - pts_ref)/100000
                # Add calculated difference to the reference real timestamp
                # (generates real frame datetime)
                real_frame_dt = datetime.fromtimestamp(
                    conv_ref) + td(seconds=diff_ref_to_frame)
                # Convert real frame datetime to real epoch timestamp and store it
                real_frame_ts[frame] = int(real_frame_dt.timestamp()*1000)

    ###############################
    # Prune frames (select only the frames that are within the video interval)
    start_to_frame_diff = {}
    frame_to_end_diff = {}

    for tstamp in real_frame_ts:
        # Calculate difference between frame real ts and video start ts
        diff_to_start = real_frame_ts[tstamp] - data_json['video']['start']
        # Calculate difference between frame real ts and video end ts
        diff_to_end = real_frame_ts[tstamp] - data_json['video']['end']
        # Store only the frames that are after the video start ts
        if diff_to_start >= 0:
            start_to_frame_diff[tstamp] = diff_to_start
        # Store only the frames that are before the video end ts
        if diff_to_end < 0:
            frame_to_end_diff[tstamp] = diff_to_end

    # Determine video frame interval
    # Calculate nearest frame from video start ts
    start_frame = min(start_to_frame_diff, key=start_to_frame_diff.get)
    # Calculate nearest frame from video end ts
    end_frame = max(frame_to_end_diff, key=frame_to_end_diff.get)

    # Compile all frames that correspond to the video interval
    video_frames_ts = {frame: real_frame_ts[frame] for frame in real_frame_ts.keys(
    ) if frame in list(range(start_frame, end_frame+1))}

    ###############################
    # Convert real frame timestamps into relative video timestamps (format: H:M:S:ms)
    frame_ts_chb = {}

    # Generate datetime value for video start timestamp
    video_start_dt = datetime.fromtimestamp(data_json['video']['start']/1000.0)

    for frame in video_frames_ts:
        # Generate datetime value for current frame
        video_frame_dt = datetime.fromtimestamp(video_frames_ts[frame]/1000.0)

        # Calculate delta between datetime values
        delta = video_frame_dt-video_start_dt
        delta_new = str(delta).replace(".", ":")
        # Store relative video timestamp for each frame and its corresponding CameraViewBlocked value
        frame_ts_chb[delta_new] = frame_camera_view[frame]

    # FOR DEBUG
    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (
        bytes(json.dumps(frame_ts_chb, ensure_ascii=False, indent=4).encode('UTF-8')))

    # Defining s3 path to store concatenated metadata full json
    if "srxdriverpr" in stream_name:
        s3_folder = container_services.sdr_folder['driver_pr']
    else:
        s3_folder = container_services.sdr_folder['debug']

    s3_file_extension = '_video_sync_info.json'
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    key_full_metadata = s3_folder + s3_filename + s3_file_extension

    # Upload final concatenated file
    container_services.upload_file(s3_client,
                                   concatenated_file,
                                   container_services.raw_s3,
                                   key_full_metadata)

    return s3_file_extension, start_frame, end_frame

def json_raise_on_duplicates(ordered_pairs):
    """Convert duplicate keys to JSON array or if JSON objects, merges them."""
    d = {}
    for (k, v) in ordered_pairs:
        if k in d:
            if type(d[k]) is dict and type(v) is dict:
                for(sub_k, sub_v) in v.items():
                    d[k][sub_k] = sub_v
            elif type(d[k]) is list:
                d[k].append(v)
            else:
                d[k] = [d[k],v]
        else:
           d[k] = v
    return d



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
    Returns:
        metadata_available {string} -- [If the metadata associated to the
                                        recording was successfully retrieved
                                        from RCC and concatenated into a
                                        metadata full file, the value of
                                        this variable is "Yes", otherwise
                                        (i.e. if the process failed at some
                                        point), its value is "No"]
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
    # 'rcc-dev-device-data'
    bucket_origin = container_services.rcc_info["s3_bucket"]

    # Initialises the variable to flag the
    # availability of the metadata files
    metadata_available = "Yes"

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    # "arn:aws:iam::213279581081:role/dev-DevCloud"
    s3_role = container_services.rcc_info["role"]
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

    # Info from received message (MessageAttributes parameter)
    rec_prefix = dict_attr['recordingId']['Value']
    device = dict_attr['deviceId']['Value']
    tenant = dict_attr['tenant']['Value']
    recorder = dict_attr['recorder']['Value']

    ###########################################################################################################
    # NOTE: S3 RCC Naming convention fix
    # TODO: Align with RCC about the recorder value
    recorder = rec_prefix.split("-")[0]
    ###########################################################################################################

    # Info from received message (Message parameter)
    stream_name = dict_body['streamName']
    epoch_from = dict_body['footageFrom']
    epoch_to = dict_body['footageTo']
    upload_start = dict_body['uploadStarted']
    upload_end = dict_body['uploadFinished']

    # Convert start timestamp (Metadata) to datetime
    meta_start_time = datetime.fromtimestamp(upload_start/1000.0)
    # Round down to exact hour (i.e. 0min 0s)
    round_start_time = meta_start_time.replace(
        microsecond=0, second=0, minute=0)

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

        ##################################################################
        # NOTE: S3 RCC Naming convention fix
        next_year = next_time.year
        next_month = f"{next_time.month:02}"
        next_day = f"{next_time.day:02}"
        next_hour = f"{next_time.hour:02}"
        ##################################################################

        # Construct timestamp part of the s3 path (folder)
        time_path = "year={}/month={}/day={}/hour={}".format(next_year,
                                                             next_month,
                                                             next_day,
                                                             next_hour)

        # Build s3 key prefix
        key_prefix = tenant + "/" + device + "/" + \
            time_path + "/" + recorder + "_" + rec_prefix

        # Get list of all files with the same key prefix as the one
        # received on the message
        response_list = rcc_s3.list_objects_v2(
            Bucket=bucket_origin,
            Prefix=key_prefix
        )

        # Check if response_list is not empty
        if response_list['KeyCount'] == 0:
            logging.info(
                "\nWARNING: No metadata files with prefix: %s were found!!", key_prefix)
            continue

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
                json_temp = json.loads(metadata_file.decode("utf-8"), object_pairs_hook=json_raise_on_duplicates)

                # Add filename to the json file (for debug)
                json_temp["filename"] = file_entry['Key']

                # Store json file on the dictionary based on the index
                files_dict[chunks_total] = json_temp

                # Increase counter for number of files received
                chunks_total += 1

                logging.info(files_dict)

    # Check if there are partial chunk MDF files
    if not files_dict or chunks_total == 0:
        logging.info(
            "\n######################## Exception #########################")
        logging.info(
            "WARNING: No valid metadata files with prefix: %s were found!!", key_prefix)
        logging.info("Please check files_dict dictionary content below:")
        logging.info(files_dict)
        logging.info(
            "############################################################\n")
        metadata_available = "No"
        sync_file_ext = ""
        return metadata_available, sync_file_ext

    # Initialise dictionary that will comprise all concatenated info
    final_dict = {}

    # Use the resolution of the first file
    # NOTE: assumption -> resolution is the same for all received files
    final_dict['resolution'] = files_dict[0]['resolution']

    # Collects all start and end timestamps from each file entry (chunk parameter)
    starts_list = []
    ends_list = []

    for item in files_dict.keys():
        starts_list.append(int(files_dict[item]['chunk']['pts_start']))
        ends_list.append(int(files_dict[item]['chunk']['pts_end']))

    # Defines chunk start point as the lowest starting timestamp and
    # end point as the highest ending timestamp
    final_dict['chunk'] = {
        "pts_start": min(starts_list),
        "pts_end": max(ends_list)
    }

    #############################################
    compact_mdf = {}
    start_frame = 0
    end_frame = 0
    #############################################

    #############################################
    # Frames Concatenation Process
    try:
        final_dict['frame'] = []
        for m in files_dict:
            for current_frame in files_dict[m]['frame']:
                final_dict['frame'].append(current_frame)

        # Sort frames by number
        newlist = sorted(final_dict["frame"],
                         key=lambda x: int(itemgetter("number")(x)))
        final_dict["frame"] = newlist

    except Exception:
        logging.info(
            "\n######################## Exception #########################")
        logging.exception(
            "ERROR: The following error occured during the concatenation process:")
        logging.info(
            "############################################################\n")
        metadata_available = "No"
        sync_file_ext = ""
        return metadata_available, sync_file_ext

    #############################################
    # Generate and store compact mdf json
    try:
        logging.info("Generating metadata compact file..")
        compact_mdf = generate_compact_mdf_metadata(container_services,
                                                    s3_client,
                                                    epoch_from,
                                                    epoch_to,
                                                    files_dict,
                                                    stream_name)
        logging.info("Metadata compact file created!\n")

    except Exception:
        logging.info(
            "\n######################## Exception #########################")
        logging.exception(
            "ERROR: The following error occured during the processing of the compact metadata:")
        logging.info("\nMetadata compact file not created!")
        logging.info(
            "############################################################\n")
        sync_file_ext = ""

    #############################################
    # Generate and store video sync json
    try:
        logging.info("Generating Video sync info file..")
        sync_file_ext, start_frame, end_frame = generate_sync_data(container_services,
                                                                   s3_client,
                                                                   epoch_from,
                                                                   epoch_to,
                                                                   compact_mdf,
                                                                   stream_name)
        logging.info("Video sync info file created!\n")

    except Exception:
        logging.info(
            "\n######################## Exception #########################")
        logging.exception(
            "ERROR: The following error occured during the video data sync process:")
        logging.info("\nVideo sync info file not created!")
        logging.info(
            "############################################################\n")
        sync_file_ext = ""

    ##########################################
    if compact_mdf and end_frame != 0:
        # Add CHC event periods to MDF file
        final_dict['chc_periods'] = calculate_chc_periods(
            compact_mdf, start_frame, end_frame)
    else:
        final_dict['chc_periods'] = []

    logging.info("Generating metadata full file..")
    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (
        bytes(json.dumps(final_dict, ensure_ascii=False, indent=4).encode('UTF-8')))

    # Defining s3 path to store concatenated metadata full json
    if "srxdriverpr" in stream_name:
        s3_folder = container_services.sdr_folder['driver_pr']
    else:
        s3_folder = container_services.sdr_folder['debug']

    s3_file_extension = '_metadata_full.json'
    s3_filename = stream_name + "_" + str(epoch_from) + "_" + str(epoch_to)
    key_full_metadata = s3_folder + s3_filename + s3_file_extension

    # Upload final concatenated file
    container_services.upload_file(s3_client,
                                   concatenated_file,
                                   container_services.raw_s3,
                                   key_full_metadata)
    logging.info("Metadata full file created!\n")
    ##########################################

    return metadata_available, sync_file_ext

def calculate_chc_periods(compact_mdf, start_frame, end_frame):
    frames_with_cv = []
    frame_times = {}

    #################################### Identify frames with cvb and cve equal to 1 #################################################################

    for frame in compact_mdf['frames']:
        if 'interior_camera_health_response_cvb' in frame['signals'] and 'interior_camera_health_response_cve' in frame['signals'] and 'timestamp' in frame and (frame['signals']['interior_camera_health_response_cvb'] == '1' or frame['signals']['interior_camera_health_response_cve'] == '1'):
            if frame['number'] >= start_frame and frame['number'] <= end_frame:
                frames_with_cv.append(frame['number'])
                frame_times[frame['number']] = frame['timestamp']

    #################################### Group frames into events with tolerance #####################################################################

    frame_groups = group_frames_to_events(frames_with_cv, 2)

    #########  Duration calculation  #################################################################################################################
    chc_periods = []
    for frame_group in frame_groups:
        entry = {}
        entry['frames'] = frame_group
        entry['duration'] = (frame_times[frame_group[-1]] -
                             frame_times[frame_group[0]])/100000
        chc_periods.append(entry)

    return chc_periods

def group_frames_to_events(frames, tolerance):
    groups = []

    if len(frames) < 1:
        return groups

    entry = []
    for i in range(0, len(frames)):
        entry.append(frames[i])
        if i == (len(frames) - 1) or abs(frames[i + 1] - frames[i]) > tolerance:
            groups.append(entry)
            entry = []

    return groups

def transfer_to_devcloud(message, tenant, rcc_s3_client, qa_s3_client, container_services):
    """Transfers every JPEG mentioned in an upoad notification message, 
    downloading from RCC S3 and uploading into DevCloud S3 bucket, 
    with its timestamp added to the file name as a suffix.

    Args:
        message {dict} -- [dict with the received message content
                           (for more info please check the response syntax
                           of the Boto3 SQS.client.receive_message method)]
        tenant {str} -- [string indicating the tenant associated to the uploaded content]
        rcc_s3_client {boto3.client} -- [client used to assume a given 
                                       cross-account IAM role into RCC]
        qa_s3_client {boto3.client} -- [client used to access
                                     the DevCloud QA S3 service]
        container_services {BaseAws.shared_functions.ContainerServices}
                            -- [class containing the shared aws functions]
    Obs:
        -- whitelisting content from 'datanauts' tenant
        -- placing snapshots into 'Debug_Lync/' folder
        -- the RCC S3 path is calculated with the snapshot 'start_timestamp_ms', which
        is not the upload timestamp 'upload_from/to'. See video chunks for reference
    """

    #message_snapshots = []
    # find snapshot device
    device = message["value"]["properties"]["header"]["device_id"]

    # for all snapshots mentioned within, identify its snapshot info and save it - (current file name, timestamp to append)
    for snapshot in message["value"]["properties"]["chunk_descriptions"]:

        # find snapshot timestamp
        timestamp = snapshot["start_timestamp_ms"]
        # our SRX device is in Portugal, 1h diff to AWS
        timestamp = datetime.fromtimestamp(timestamp/1000.0) + td(hours=-1.0)

        # obtain file name
        file_name = snapshot["uuid"]

        # ensure it is a snapshot and not an .mp4 chunk
        if file_name.endswith(".jpeg"):

            # define its new name by adding the timestamp as a suffix
            new_name = file_name[:-5]+"_"+str(int(timestamp.timestamp()))+".jpeg"

            # check if file is already on DevCloud (searching for its name)
            response = qa_s3_client.list_objects_v2(
                Bucket=container_services.raw_s3,  # qa-rcd-raw-video-files
                Prefix='Debug_Lync/'+new_name,
                Delimiter="/"
            )

            # if it was not found on DevCloud
            if "Contents" not in response or not response["Contents"][0]["Key"].endswith(new_name):

                # determine where to search for the file on RCC S3
                possible_locations = snapshot_path_generator(
                    tenant, device, timestamp, datetime.now())

                # for all those locations
                for folder in possible_locations:

                    try:
                        # check if the file is in the location
                        response = rcc_s3_client.list_objects_v2(
                            Bucket='rcc-prod-device-data',  # qa-rcd-raw-video-files
                            Prefix=folder+file_name,
                            Delimiter="/"
                        )
                    except Exception:
                        if folder == possible_locations[-1]:
                            logging.info("WARNING: Could not find %s in RCC S3"% (file_name))

                    # if it gets found on the location
                    # stored where expected
                    if "Contents" in response and response["Contents"][0]["Key"].endswith(file_name):

                        # download jpeg from RCC into container file-system
                        rcc_s3_client.download_file(
                            container_services.rcc_info["s3_bucket"], folder+file_name, new_name)

                        # upload to DevCloud
                        with open(new_name, "rb") as f:
                            snapshot_bytes = f.read()
                            container_services.upload_file(qa_s3_client,
                                                           snapshot_bytes,
                                                           container_services.raw_s3,
                                                           'Debug_Lync/'+new_name)

                        # and delete it from the container
                        if os.path.exists(new_name):
                            os.remove(new_name)
                        break
        else:
            logging.info("Found something other than a snapshot: %s"%(file_name))

def snapshot_path_generator(tenant: str, device: str, start: ST, end: ST):
    """Generate the list of possible folders between the range of two timestamps

    Args:
        tenant (str): The device tenant
        device (str): The device identifier
        start (ST): The lower limit of the time range
        end (ST, optional): The upper limit of the time range. Defaults to datetime.now().

    Returns:
        [str]: List with all possible paths between timestamp bounds, sorted old to new. 
    """
    if not tenant or not device or not start or not end: return []
    # cast to datetime format
    start_timestamp = datetime.fromtimestamp(start/1000.0) if type(start) != datetime else start
    end_timestamp = datetime.fromtimestamp(end/1000.0) if type(end) != datetime else end
    if int(start.timestamp()) < 0 or int(end.timestamp()) < 0: return []

    dt = start_timestamp
    times = []
    # for all hourly intervals between start_timestamp and end_timestamp
    while dt <= end_timestamp or (dt.hour == end_timestamp.hour):
        # append its respective path to the list
        times.append("%s/%s/year=%s/month=%s/day=%s/hour=%s/" %(tenant, device, dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), dt.strftime("%H")))
        dt = dt + td(hours=1)

    # and return it
    return times

def event_type_identifier(message):
    """Receive a message and find the event type of the upload.

    Args:
        message (dict): SQS message to be parsed.

    Returns:
        eventType (str): Upload eventType. If missing returns None.
        tenant (str): Device tenant. If missing returns None.
    """
    eventType = None
    tenant = None
    message_attrib = None
    if "MessageAttributes" in message: 
        message_attrib = message["MessageAttributes"]
    else:
        body = json.loads(message["Body"])
        if "MessageAttributes" in body: 
            message_attrib = body["MessageAttributes"]
        else:
            message_ = json.loads(body["Message"])
            if "MessageAttributes" in message_: 
                message_attrib = message_["MessageAttributes"]
    if message_attrib is None: 
        logging.info("ERROR: Could not parse this message:\n")
        logging.info(message)
        return eventType, tenant
    if "eventType" in message_attrib:
        if "Value" in message_attrib["eventType"]: 
            eventType = message_attrib["eventType"]["Value"]
        elif "StringValue" in message_attrib["eventType"]: 
            eventType = message_attrib["eventType"]["StringValue"]
    if 'tenant' in message_attrib:
        if "Value" in message_attrib["tenant"]: 
            tenant = message_attrib["tenant"]["Value"]
        elif "StringValue" in message_attrib["tenant"]: 
            tenant = message_attrib["tenant"]["StringValue"]
    return eventType, tenant

def log_message(message, queue=CONTAINER_NAME):
    logging.info("\n######################################\n")
    logging.info("Message contents from %s:\n"%(queue))
    logging.info(message)
    logging.info("\n######################################\n")

def main():
    """Main function"""

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
                 CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3',region_name='eu-central-1')
    sqs_client = boto3.client('sqs',region_name='eu-central-1')
    sts_client = boto3.client('sts',region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    logging.info("\nListening to input queue(s)..\n")
    
    # prepare counters
    video_flag = MAX_CONSECUTIVE
    snapshot_flag = MAX_CONSECUTIVE
    while(True):
        # if it is time to look for videos
        if video_flag:
            video_flag += -1
            # Check input SQS queue for new messages
            message = container_services.listen_to_input_queue(sqs_client)

            if message:
                # save some messages as examples for development
                log_message(message, CONTAINER_NAME)
                # Get and store kinesis video clip
                rec_data, hq_data = transfer_kinesis_clip(s3_client,
                                                        sts_client,
                                                        container_services,
                                                        message)

                # Checks if recording received is valid for
                # HQ data request
                if hq_data:
                    # Send message to secondary input queue (HQ_Request)
                    # of Selector container
                    hq_selector_queue = container_services.sqs_queues_list["HQ_Selector"]
                    container_services.send_message(sqs_client,
                                                    hq_selector_queue,
                                                    hq_data)

                # Checks if recording received is valid
                if rec_data:
                    # Concatenate all metadata related to processed clip
                    meta_available, sync_ext = concatenate_metadata_full(s3_client,
                                                                        sts_client,
                                                                        container_services,
                                                                        message)

                    # Add parameter with info about metadata availability
                    rec_data["MDF_available"] = meta_available

                    # Add parameter with video sync data
                    rec_data["sync_file_ext"] = sync_ext

                    # Send message to input queue of metadata container
                    metadata_queue = container_services.sqs_queues_list["Metadata"]
                    container_services.send_message(sqs_client,
                                                    metadata_queue,
                                                    rec_data)

                # Delete message after processing
                container_services.delete_message(sqs_client,
                                                message['ReceiptHandle'])
            # if no video message was found, look for snapshots
            else: video_flag = 0
            if video_flag == 0: snapshot_flag=MAX_CONSECUTIVE
        # if it is time to look for snapshots
        if snapshot_flag:
            snapshot_flag += -1
            # process for snaphots
            # access the old selector queue "qa-terraform-queue-selector"
            api_sqs_queue = container_services.sqs_queues_list['Selector']

            # get a message from the Selector SQS queue
            message = container_services.listen_to_input_queue(
                sqs_client, api_sqs_queue)

            # diferentiate between message type (video/snapshot)
            if message:
                # save some messages as examples for development
                log_message(message, api_sqs_queue)
                # the message body comes formatted as a string, needs to be parsed to json
                body = json.loads(message["Body"])
                # the message _in_ the body is also formatted as a string, needs to be parsed to json
                # Verify if the message exists inside Message field
                event_type,tenant = event_type_identifier(message)
                if "Attributes" in message and "SentTimestamp" in message["Attributes"]:
                    logging.info("Message timestamp: %s"%(datetime.fromtimestamp(int(message["Attributes"]["SentTimestamp"])/1000.0)))
                elif "Timestamp" in message:
                    logging.info("Message timestamp: %s"%(message["Timestamp"]))
                if event_type =="com.bosch.ivs.videorecorder.UploadRecordingEvent":
                    if tenant != "TEST_TENANT":
                        message_ = body["Message"] if "Message" in body else body
                        if type(message_) == str: message_ = json.loads(body["Message"])
                        if "chunk_descriptions" in message_["value"]["properties"].keys():
                            logging.info(
                                "Tenant is %s, processing files..." % (tenant))

                            # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
                            # "arn:aws:iam::213279581081:role/dev-DevCloud"
                            s3_role = container_services.rcc_info["role"]
                            sts_session = "AssumeRoleSession2"

                            # Requests credentials to assume specific cross-account role
                            assumed_role_object = sts_client.assume_role(RoleArn=s3_role,
                                                                        RoleSessionName=sts_session)
                            role_creds = assumed_role_object['Credentials']

                            # Create a S3 client with temporary STS credentials
                            # to enable cross-account access
                            rcc_s3_client = boto3.client('s3',
                                                        region_name='eu-central-1',
                                                        aws_access_key_id=role_creds['AccessKeyId'],
                                                        aws_secret_access_key=role_creds['SecretAccessKey'],
                                                        aws_session_token=role_creds['SessionToken'])

                            # transfer snapshots mentioned in the message
                            transfer_to_devcloud(
                                message_, tenant, rcc_s3_client, s3_client, container_services)
                        else:
                            logging.info("WARNING: No files found in message")
                            logging.info(message)
                    else:
                        logging.info(
                            "WARNING: Message skipped (Tenant is %s)", tenant)
                else:
                    logging.info("WARNING: Message skipped (MessageType is %s)", event_type)
            # if no snapshot message was found, look for videos
            else: snapshot_flag = 0
            if snapshot_flag == 0: video_flag=MAX_CONSECUTIVE
if __name__ == '__main__':
    main()
