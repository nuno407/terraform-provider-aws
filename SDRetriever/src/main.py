"""Anonymize container script"""
import json
import logging
import boto3
from baseaws.shared_functions import ContainerServices
from datetime import timedelta as td, datetime
import subprocess
from operator import itemgetter

CONTAINER_NAME = "SDRetriever"    # Name of the current container
CONTAINER_VERSION = "v5.0"      # Version of the current container


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

    record_data = {}


    ###########################################
    #DEBUG
    tenant = dict_attr['tenant']['Value']
    if tenant == "TEST_TENANT":
        logging.info("\nWARNING: Message skipped (TEST_TENANT | %s)", dict_body['streamName'])
        return record_data

    logging.info(message)
    ##########################################


    try:
        # Info from received message
        stream_name = dict_body['streamName']

        epoch_from = dict_body['footageFrom']
        start_time = datetime.fromtimestamp(epoch_from/1000.0).strftime('%Y-%m-%d %H:%M:%S')

        epoch_to = dict_body['footageTo']
        end_time = datetime.fromtimestamp(epoch_to/1000.0).strftime('%Y-%m-%d %H:%M:%S')

    except Exception as e:
        logging.info("\nWARNING: Message (id: %s) contains unsupported info! Please check the error below:", message['MessageId'])
        logging.info(e)
        logging.info("\n")
        return record_data

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    selector = 'PRODUCER_TIMESTAMP'
    stream_role = container_services.rcc_info["role"] #"arn:aws:iam::213279581081:role/dev-DevCloud"
    clip_ext = ".mp4"
    sts_session = "AssumeRoleSession1"
    
    # Defining s3 path to store KVS clip
    if "srxdriverpr" in stream_name:
        s3_folder = container_services.sdr_folder['driver_pr']
    else:
        s3_folder = container_services.sdr_folder['debug']

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
    video_resolution = width + "x"+ height

    # Get video duration
    video_seconds = round(float(video_info["format"]["duration"]))
    video_duration = str(td(seconds=video_seconds))

    # Build dictionary with info to store on DB (Recording collection)
    record_data["_id"] = s3_filename
    record_data["s3_path"] = container_services.raw_s3 + "/" + s3_path
    record_data["recording_overview"] = {
                                         'length': video_duration,
                                         'time': str(start_time),
                                         'deviceID': device,
                                         'resolution': video_resolution,
                                         '#snapshots': "0",
                                         'snapshots_paths': {}
                                        }

    return record_data

def generate_compact_mdf_metadata(container_services, s3_client, epoch_from, epoch_to, files_dict, stream_name):
    """TODO

    Arguments:
        TODO
    """
    aux_list = []

    key_set = set(("CameraViewBlocked", "cvb", "cve"))
    
    final_info = {
                "partial_timestamps": {},
                "frames": [],
                "video": {
                            "start": epoch_from,
                            "end": epoch_to
                }
            }

    for partial_mdf in files_dict:

        aux_frames_list = []

        # Partial timestamps  ###############################################
        first_split = files_dict[partial_mdf]["filename"].split("._stream2_")
        second_split = first_split[1].split("_")
        human_timestamp = second_split[0]

        # Convert into datetime and then into epoch timestamp
        datetime_object = datetime.strptime(human_timestamp, "%Y%m%d%H%M%S")
        chunk_epoch_start = int(datetime_object.timestamp())

        # Frames  ###########################################################
        for frame in files_dict[partial_mdf]["frame"]:

            aux_frames_list.append(int(frame["number"]))

            frame_data = {
                        "number": int(frame["number"]),
                        "timestamp": int(frame["timestamp"])
            }

            if 'objectlist' in frame.keys():
                for item in frame['objectlist']:
                    if item['id'] == '1':
                        frame_data["CameraViewBlocked"] = item['floatAttributes'][0]['value']
                    if item['id'] == '4':
                        frame_data["cvb"] = item['floatAttributes'][0]['value']
                    if item['id'] == '5':
                        frame_data["cve"] = item['floatAttributes'][0]['value']
                        
            if key_set.issubset(frame_data.keys()):
                aux_list.append(frame_data)

        # Partial timestamps full info #########################################
        final_info["partial_timestamps"][human_timestamp] = {
            "filename": files_dict[partial_mdf]["filename"], 
            "pts_start": int(files_dict[partial_mdf]["chunk"]["pts_start"]),
            "converted_time": chunk_epoch_start,
            "frames_list": aux_frames_list
        }

    final_info["frames"] = sorted(aux_list, key=lambda x: int(itemgetter("number")(x)))

    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (bytes(json.dumps(final_info, ensure_ascii=False, indent=4).encode('UTF-8')))

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
    """TODO

    Arguments:
        TODO
    """
    # Collect frame data and store it in separate dictionaries
    frame_ts = {}
    frame_chb = {}

    for frame in data_json['frames']:
        # Collect relative timestamp for each frame
        frame_ts[frame['number']] = frame['timestamp']
        # Collect CameraViewBlocked value for each frame
        frame_chb[frame['number']] = frame['CameraViewBlocked']

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
                real_frame_dt = datetime.fromtimestamp(conv_ref) + td(seconds=diff_ref_to_frame)
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
    video_frames_ts = {frame: real_frame_ts[frame] for frame in real_frame_ts.keys() if frame in list(range(start_frame, end_frame+1))}

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
        frame_ts_chb[delta_new] = float(frame_chb[frame])

    ############################################################################################################ FOR DEBUG
    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (bytes(json.dumps(frame_ts_chb, ensure_ascii=False, indent=4).encode('UTF-8')))

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
    bucket_origin = container_services.rcc_info["s3_bucket"] #'rcc-dev-device-data'

    # Initialises the variable to flag the
    # availability of the metadata files
    metadata_available = "Yes"

    # TODO: ADD THE BELLOW INFO TO A CONFIG FILE
    s3_role = container_services.rcc_info["role"] #"arn:aws:iam::213279581081:role/dev-DevCloud"
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
            metadata_available = "No"
            sync_file_ext = ""
            return metadata_available, sync_file_ext

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

                # Add filename to the json file (for debug)
                json_temp["filename"] = file_entry['Key']

                # Store json file on the dictionary based on the index
                files_dict[chunks_total] = json_temp

                # Increase counter for number of files received
                chunks_total += 1

    # Check if there are partial chunk MDF files
    if not files_dict or chunks_total == 0:
        logging.info("\nWARNING: No valid metadata files with prefix: %s were found!!\n", key_prefix)
        logging.info("Please check files_dict dictionary content below:\n")
        logging.info(files_dict)
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
        newlist = sorted(final_dict["frame"], key=lambda x: int(itemgetter("number")(x)))
        final_dict["frame"] = newlist

    except Exception as e:
        logging.info("\nWARNING: The following error occured during the concatenation process:\n")
        logging.info(e)
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

    except Exception as e:
        logging.info("\nWARNING: The following error occured during the processing of the compact metadata:\n")
        logging.info(e)
        logging.info("WARNING: Metadata compact file not created!\n")
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

    except Exception as e:
        logging.info("\nWARNING: The following error occured during the video data sync process:\n")
        logging.info(e)
        logging.info("WARNING: Video sync info file not created!\n")
        sync_file_ext = ""

    ##########################################
    if compact_mdf and end_frame != 0:
        # Add CHC event periods to MDF file
        final_dict['chc_periods'] = calculate_chc_periods(compact_mdf, start_frame, end_frame)
    else:
        final_dict['chc_periods'] = []

    logging.info("Generating metadata full file..")
    # Convert concatenated dictionary into json and then into bytes so
    # that it can be uploaded into the S3 bucket
    concatenated_file = (bytes(json.dumps(final_dict, ensure_ascii=False, indent=4).encode('UTF-8')))

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
        if 'cvb' in frame and 'cve' in frame and 'timestamp' in frame and (frame["cvb"] == "1" or frame["cve"] == "1"):
            if frame["number"] >= start_frame and frame["number"] <= end_frame:
                frames_with_cv.append(frame["number"])
                frame_times[frame["number"]] = frame["timestamp"]

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


if __name__ == '__main__':
    main()
