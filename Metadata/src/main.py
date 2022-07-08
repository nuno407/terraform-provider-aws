"""Metadata container script"""
import json
import logging
import os
from typing import Optional, Tuple
import boto3
import pytz
from baseaws.shared_functions import ContainerServices, GracefulExit
from baseaws.chc_periods_functions import calculate_chc_periods, generate_compact_mdf_metadata
from datetime import datetime
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from baseaws.voxel_functions import create_dataset, update_sample

CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v6.2"     # Version of the current container

VIDEO_FORMATS = ['avi','mp4']
IMAGE_FORMATS = ['jpeg','jpg','png']

def upsert_recording_item(message: dict, table_rec: Collection)->Optional[dict]:
    """Inserts a new item on the recordings collection 

    Arguments:
        message {dict} -- [info set from the sqs message received
                        that will be used to populate the newly
                        created DB item]
        table_rec {MongoDB collection object} -- [Object used to
                                                    access/update the
                                                    recordings collection
                                                    information]
    """

    # check that data has at least the absolute minimum of required fields
    if not all(k in message for k in ('_id', 's3_path')):
        logging.error("\nNot able to create an entry because _id or s3_path is missing!")
        return None

    # Build item structure and add info from msg received
    
    s3split = message["s3_path"].split("/")
    filetype = s3split[-1].split(".")[-1]

    if filetype in IMAGE_FORMATS:
        media_type = 'image'
    else:
        media_type = 'video'

    recording_overview: dict = message.get('recording_overview', {})
    recording_item = {}
    recording_item['video_id'] = message['_id']
    recording_item['filepath'] = 's3://' + message['s3_path']
    recording_item['MDF_available'] = message.get('MDF_available', 'No')
    if len(recording_overview) > 0: recording_item['recording_overview'] = {k:v for (k, v) in recording_overview.items() if k!='resolution'}
    if 'resolution' in recording_overview: recording_item['resolution'] = recording_overview['resolution']
    recording_item['_media_type'] = media_type
    recording_item['chunk'] = 'Chunk0'
    
    recording_item['recording_overview']['tenantID'] = message['_id'].split("_",1)[0]
    recording_item['recording_overview']['number_chc_events'] = 0
    recording_item['recording_overview']['chc_duration'] = 0

    try:
        # Upsert previous built item on the Recording collection
        table_rec.update_one({'video_id': message["_id"]}, {'$set': recording_item}, upsert=True)
        # Create logs message
        logging.info("Recording DB item (video_id: %s) upserted!", message["_id"])
    except Exception:
        logging.info("\n######################## Exception #########################")
        logging.exception("Warning: Unable to create or replace recording item for id: %s", message["_id"])
        logging.info("############################################################\n")

    return recording_item

def calculate_chc_events(chc_periods):
    duration = 0.0
    number = 0
    for period in chc_periods:
        duration += period['duration']
        if period['duration'] > 0.0:
            number += 1

    return number, duration

def upsert_signals_item(video_id: str, s3_path: str, sync_file_extension: str, table_sig: Collection)->Optional[dict]:
    # Create S3 client to download metadata
    s3_client = boto3.client('s3',
                region_name='eu-central-1')

    s3_bucket, video_key = s3_path.split("/", 1)
    s3_key = video_key.split(".")[0] + '_metadata_full.json'

    # Download metadata json file
    get_mdf_response = s3_client.get_object(
                                        Bucket=s3_bucket,
                                        Key=s3_key
                                    )

    # Decode and convert file contents into json format
    mdf = json.loads(get_mdf_response['Body'].read().decode("utf-8"))

    # Add array from metadata full file to created item
    signals_item = {
                'recording': video_id,
                'source': "MDF",
                'signals': {},
                'CHC_periods': [],
                }

    # NOTE: Condition added due to some MDF files still not having this info
    if 'chc_periods' in mdf:
        chc_periods = mdf['chc_periods']
        signals_item['CHC_periods'] = chc_periods
        chc_number, chc_duration = calculate_chc_events(chc_periods)
        signals_item['number_chc_events'] = chc_number
        signals_item['chc_duration'] = chc_duration

    # Add sync data
    s3_key = video_key.split(".")[0] + sync_file_extension

    # Download metadata json file
    get_sync_response = s3_client.get_object(
                                    Bucket=s3_bucket,
                                    Key=s3_key
                                )

    # Decode and convert file contents into json format
    synchronized_signals = json.loads(get_sync_response['Body'].read().decode("utf-8"))

    signals_item['signals'] = synchronized_signals

    try:
        # Upsert signals item
        table_sig.update_one({'recording': video_id}, {'$set': signals_item}, upsert=True)
        # Create logs message
        logging.info("Signals DB item (video_id: %s) upserted!", video_id)
    except Exception:
        logging.info("\n######################## Exception #########################")
        logging.exception("Warning: Unable to create or replace signals item for id: %s", video_id)
        logging.info("############################################################\n")
    return signals_item

def update_pipeline_db(video_id: str, message: dict, table_pipe: Collection, source: str)->Optional[dict]:
    """Inserts a new item (or updates it if already exists) on the
    pipeline execution collection

    Arguments:
        video_id {string} -- [Name of the recording (coincides with the
                                name of the item to be created/updated
                                in this function]
        message {dict} -- [info set from the received sqs message
                        that is used to populate the created/updated
                        item]
        table_pipe {MongoDB collection object} -- [Object used to
                                                    access/update the
                                                    pipeline execution
                                                    collection information]
        source {string} -- [Name of the container which is the source
                            of the info received on the sqs message]
    """
    # check that data has at least the absolute minimum of required fields
    if not all(k in message for k in ('data_status', 's3_path')):
        logging.error('Skipping pipeline collection update for %s as not all required fields are present in the message.', video_id)
        return None

    # Initialise pipeline item to upsert
    timestamp = str(datetime.now(tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

    upsert_item = {
        '_id': video_id,
        'data_status': message['data_status'],
        'from_container': CONTAINER_NAME,
        'info_source': source,
        'last_updated': timestamp,
        's3_path': message['s3_path']
    }
    if 'processing_steps' in message: 
        upsert_item['processing_list']= message['processing_steps']

    try:
        # Upsert pipeline executions item
        table_pipe.update_one({'_id': video_id}, {'$set': upsert_item}, upsert=True)
        # Create logs message
        logging.info("Pipeline Exec DB item (Id: %s) updated!", video_id)
    except Exception:
        logging.info("\n######################## Exception #########################")
        logging.exception("Warning: Unable to create or replace pipeline executions item for id: %s", video_id)
        logging.info("############################################################\n")

    ## Voxel51 code
    s3split = message["s3_path"].split("/")
    bucket_name = s3split[0]
    
    filetype = s3split[-1].split(".")[-1]

    if filetype in IMAGE_FORMATS:
        bucket_name = bucket_name+"_snapshots"
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-5]+'_anonymized.'+filetype
    elif filetype in VIDEO_FORMATS:
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-4]+'_anonymized.'+filetype
    else:
        raise ValueError("Unknown file format %s"%filetype)
    sample = upsert_item
    sample["video_id"] = video_id
    sample["s3_path"] = anonymized_path
        
    try:
        # Create dataset with the bucket_name if it doesn't exist
        create_dataset(bucket_name)        
        #Add  the video to the dataset
        update_sample(bucket_name,sample)
        # Create logs message
        logging.info("Voxel sample with (Id: %s) created!", bucket_name)
    except Exception:
        logging.info("\n######################## Exception #########################")
        logging.exception("Warning: Unable to create dataset with (Id: %s) !", bucket_name)
        logging.info("############################################################\n")

    return upsert_item

def download_and_synchronize_chc(bucket: str, key: str)->Tuple[dict, dict]:
        # Create S3 client to download metadata
        s3_client = boto3.client('s3',
                    region_name='eu-central-1')

        # Download metadata json file
        mdf_download = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )

        # Decode and convert file contents into json format
        mdf = json.loads(mdf_download['Body'].read().decode("utf-8"))

        ############################################################################################################################################################################################################################
        ############################################################################################################################################################################################################################

        # Collect frame data and store it in separate dictionaries
        frame_ts = {}
        frame_signals: dict = {}

        for frame in mdf['frame']:

            if 'objectlist' in frame.keys():
                # Collect relative timestamp for each frame
                frame_ts[frame['number']] = frame['timestamp64']
                frame_signals[frame['number']] = {}

                for item in frame['objectlist']:
                    if 'boolAttributes' in item:
                        for attribute in item['boolAttributes']:
                            frame_signals[frame['number']][attribute['name']] = (
                                attribute['value'] == 'true')
                    if 'floatAttributes' in item:
                        for attribute in item['floatAttributes']:
                            frame_signals[frame['number']][attribute['name']
                                                ] = float(attribute['value'])
                    if 'integerAttributes' in item:
                        for attribute in item['integerAttributes']:
                            frame_signals[frame['number']][attribute['name']] = int(
                                attribute['value'])

        frame_ref_ts = int(frame_ts[list(frame_ts.keys())[0]])
        frame_ref_dt = datetime.fromtimestamp(frame_ref_ts/1000.0)
        ###############################

        frame_ts_signals = {}
        for frame, value_ts in frame_ts.items():
            frame_dt = datetime.fromtimestamp(int(value_ts)/1000.0)
            delta = str(frame_dt-frame_ref_dt)
            video_ts = delta.replace(".", ":")
            frame_ts_signals[video_ts] = frame_signals[frame]

        ###############################
        ### CHC periods - outdated! ###
        compact_mdf_metadata = generate_compact_mdf_metadata(mdf)
        chc_periods = calculate_chc_periods(compact_mdf_metadata)

        return frame_ts_signals, chc_periods

def process_outputs(video_id: str, message: dict, table_algo_out: Collection, table_sig: Collection, source: str):
    """Inserts a new item on the algorithm output collection and, if
    there is a CHC file available for that item, processes
    that info and adds it to the item (plus updates the respective
    signals collection item with these signals)

    Arguments:
        message {dict} -- [info set from the received sqs message
                        that is used to populate the created item]
        table_algo_out {MongoDB collection object} -- [Object used to
                                                    access/update the
                                                    algorithm output
                                                    collection information]
        table_sig {MongoDB collection object} -- [Object used to
                                                    access/update the
                                                    signals
                                                    collection information]
        rec_object {MongoDB document} -- [Object used to
                                                    read recording information for Voxel]
        timestamp {string} -- [Current timestamp to be used on status
                                update logs]
        unique_id {string} -- [Name of the recording (coincides with the
                                name of the item to be created/updated
                                in this function]
        source {string} -- [Name of the container which is the source
                            of the info received on the sqs message]
    """
    # check that data has at least the absolute minimum of required fields
    if not (all(k in message for k in ('output', 's3_path')) and 'bucket' in message['output']):
        logging.error("\nNot able to create an entry because output or s3_path is missing!")
        return

    # Initialise variables used in item creation
    outputs = message['output']
    run_id = video_id + '_' + source
    
    # Item creation (common parameters)
    algo_item: dict = {
                '_id': run_id,
                'algorithm_id': source,
                'pipeline_id': video_id,
                'output_paths': {}
            }

    # Check if there is a metadata file path available
    if 'meta_path' in outputs and outputs['meta_path'] != "-":
        metadata_path = outputs['bucket'] + '/' + outputs['meta_path']
        algo_item['output_paths']["metadata"] = metadata_path

    # Check if there is a video file path available
    if "media_path" in outputs and outputs['media_path'] != "-":
        media_path = outputs['bucket'] + '/' + outputs['media_path']
        algo_item['output_paths']["video"] = media_path
    
    # Compute results from CHC processing
    if source == "CHC" and 'meta_path' in outputs and 'bucket' in outputs:
        synchronized, chc_periods = download_and_synchronize_chc(outputs['bucket'], outputs['meta_path'])

        algo_item['results'] = {}
        # Add video sync data processed from ivs_chain metadata file to created item
        algo_item['results']['CHBs_sync'] = synchronized
        algo_item['results']['CHC_periods'] = chc_periods
        
        signals_item = {
                    'algo_out_id': run_id,
                    'recording': video_id,
                    'source': "CHC",
                    'signals': synchronized,
                    'CHC_periods': chc_periods
                    }
        try:
            # upsert signals DB item
            sig_query = {'recording': video_id, 'algo_out_id': run_id}
            table_sig.update_one(sig_query, {'$set': signals_item}, upsert=True)

            # Create logs message
            logging.info("Signals DB item (algo id: %s) updated!", run_id)

        except Exception:
            logging.info("\n######################## Exception #########################")
            logging.exception("The following exception occured during updating the signals collection with CHC output:")
            logging.info(signals_item)
            logging.info("############################################################\n")

    try:
        # Insert previously built item
        table_algo_out.insert_one(algo_item)
        logging.info("Algo Output DB item (run_id: %s) created!", run_id)

    except DuplicateKeyError:
        # Raise error exception if duplicated item is found
        # NOTE: In this case, the old item is not overriden!
        logging.info("\n######################## Exception #########################")
        logging.exception("The following exception occured during inserting output item:")
        logging.info(algo_item)
        logging.info("############################################################\n")

    ## Voxel51 code
    s3split = message["s3_path"].split("/")
    bucket_name = s3split[0]

    filetype = s3split[-1].split(".")[-1]
    anon_video_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-4]+'_anonymized.'+filetype

    if filetype == "jpeg" or filetype == "png":
        bucket_name = bucket_name+"_snapshots"

    if filetype == "jpeg":
        anon_video_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-5]+'_anonymized.'+filetype

    sample = algo_item

    sample["algorithms"] = {}

    if source == "CHC":
        sample["algorithms"][algo_item['_id']] = {"results":algo_item["results"], "output_paths":algo_item['output_paths']}
    else:
        sample["algorithms"][algo_item['_id']] = {"output_paths":algo_item['output_paths']}

        
    sample["s3_path"] = anon_video_path
    sample["video_id"] = algo_item["pipeline_id"]

    try:
        # Create dataset with the bucket_name if it doesn't exist
        create_dataset(bucket_name)
        
        #Add  the video to the dataset if it doesn't exist, otherwise update it
        update_sample(bucket_name,sample)
        
        # Create logs message
        logging.info("Voxel sample with (Id: %s) created!", bucket_name)
    except Exception:
        logging.info("\n######################## Exception #########################")
        logging.exception("Warning: Unable to create dataset with (Id: %s) !", bucket_name)
        logging.info("############################################################\n")


def upsert_data_to_db(db: Database, container_services: ContainerServices, message: dict, message_attributes: dict):
    """Main DB access function that processes the info received
    from a sqs message and calls the corresponding functions
    necessary to create/update DB items

    Arguments:
        data {dict} -- [info set from the received sqs message
                        that is used to populate the DB items]
        attributes {dict} -- [attributes from the received sqs message
                                that is used to populate DB items or to
                                define which operations are performed]
    """
    # Validate the message
    if not('s3_path' in message and
            'SourceContainer' in message_attributes and
            'StringValue' in message_attributes['SourceContainer']):
            logging.error('Skipping message due to neccessary content not being present.', message, message_attributes)
            return

    # Specify the tables to be used
    table_pipe = db[container_services.db_tables['pipeline_exec']]
    table_algo_out = db[container_services.db_tables['algo_output']]
    table_rec = db[container_services.db_tables['recordings']]
    table_sig = db[container_services.db_tables['signals']]

    # Get source container name
    source = message_attributes['SourceContainer']['StringValue']

    #################### NOTE: Recording collection handling ##################

    # generate recording entry with retrieval of SDRetriever message
    if source == "SDRetriever":
        # Call respective processing function
        recording_item = upsert_recording_item(message, table_rec)

        try:
            # Create dataset with the bucket_name if it doesn't exist
            create_dataset(bucket_name)
        
            #Add  the video to the dataset if it doesn't exist, otherwise update it
            update_sample(bucket_name,recording_item)

            # Create logs message
            logging.info("Voxel sample with (Id: %s) created!", bucket_name)
        except Exception:
            logging.info("\n######################## Exception #########################")
            logging.exception("Warning: Unable to create dataset with (Id: %s) !", bucket_name)
            logging.info("############################################################\n")

        if recording_item and recording_item.get("MDF_available", "No") == "Yes" and message.get("sync_file_ext"):
            upsert_signals_item(recording_item['video_id'], message['s3_path'], 
                                            message['sync_file_ext'], table_sig)
        else:
            logging.warning('Not creating signals entry for %s, because MDF is not available.', message['_id'])
        return

    #################### NOTE: Pipeline execution collection handling ####################

    # Get filename (id) from message received
    # Note: If source container is SDRetriever then:
    #            message["s3_path"] = bucket + key
    #       Otherwise, for all other containers:
    #            message["s3_path"] = key
    #
    recording_id = os.path.basename(message["s3_path"]).split(".")[0]

    # Call respective processing function
    update_pipeline_db(recording_id, message, table_pipe, source)

    ############################################ DEBUG MANUAL UPLOADS (TODO: REMOVE AFTERWARDS)
    ##########################################################################################
    s3split = message["s3_path"].split("/")
    filetype = s3split[-1].split(".")[-1]

    if filetype in IMAGE_FORMATS:
        media_type = 'image'
    else:
        media_type = 'video'
    # Check if item with that name already exists
    response_rec = table_rec.find_one({'video_id': recording_id})
    if response_rec:
        pass
    else:
        # Create empty item for the recording
        item_db = {
                    'video_id': recording_id,
                    'filepath': "s3://" + container_services.raw_s3 + "/" + message["s3_path"],
                    'MDF_available': "No",
                    "chunk": "Chunk0",
                    "_media_type": media_type
            }
        # Insert previous built item on the Recording collection
        table_rec.insert_one(item_db)
        # Create logs message
        logging.info("Recording DB empty item (Id: %s) created!", recording_id)
        logging.info("data:")
        logging.info(message)
    ###########################################################################################
    ###########################################################################################


    #################### NOTE: Algorithm output collection handling ####################
    # Create/Update item on Algorithm Output DB
    if 'output' in message:
        # Call respective processing function
        process_outputs(recording_id, message,
                                table_algo_out,
                                table_sig,
                                source)

def read_message(container_services: ContainerServices, body: str)->dict:
    """Copies the relay list info received from other containers and
    converts it from string into a dictionary

    Arguments:
        container_services {BaseAws.shared_functions.ContainerServices}
                        -- [class containing the shared aws functions]
        body {string} -- [string containing the body info from
                          the received message]
    Returns:
        relay_data {dict} -- [dict with the updated info for the file received
                            and to be sent via message to the output queue]
    """

    logging.info("Processing pipeline message..\n")

    # Converts message body from string to dict
    # (in order to perform index access)
    new_body = body.replace("\'", "\"")
    dict_body = json.loads(new_body)

    # currently just sends the same msg that received
    relay_data = dict_body

    container_services.display_processed_msg(relay_data["s3_path"])

    return relay_data


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

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)
    
    # initialize DB client
    db_client = container_services.create_db_client()

    # use graceful exit
    graceful_exit = GracefulExit()

    logging.info("\nListening to input queue(s)..\n\n")

    while(graceful_exit.continue_running):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            logging.info(message)
            # Processing step
            relay_list = read_message(container_services, message['Body'])

            # Insert/update data in db
            upsert_data_to_db(db_client, container_services, relay_list, message['MessageAttributes'])

            # Send message to output queue of metadata container
            output_queue = container_services.sqs_queues_list["Output"]
            container_services.send_message(sqs_client, output_queue, relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client, message['ReceiptHandle'])

if __name__ == '__main__':
    main()
