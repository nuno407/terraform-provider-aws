"""Metadata container script"""
import json
import logging
import os
from time import strftime
from typing import Optional, Tuple
from typing_extensions import Self
import boto3
import pytz
import pytimeparse
from mdfparser.chc_counter import ChcCounter
from consumer.chc_synchronizer import ChcSynchronizer
from consumer.db import Persistence
from consumer.service import RelatedMediaService
from baseaws.shared_functions import ContainerServices, GracefulExit, VIDEO_FORMATS, IMAGE_FORMATS
from datetime import datetime, timedelta
from pymongo.collection import Collection
from pymongo.database import Database
from baseaws.voxel_functions import create_dataset, update_sample

CONTAINER_NAME = "Metadata"    # Name of the current container
CONTAINER_VERSION = "v6.2"     # Version of the current container

_logger = ContainerServices.configure_logging('metadata')


def create_recording_item(message: dict, table_rec: Collection, service: RelatedMediaService) -> Optional[dict]:
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
    assert '_id' in message and 's3_path' in message

    # Build item structure and add info from msg received
    file_format = message["s3_path"].split(".")[-1]
    recording_item = dict()

    if file_format in IMAGE_FORMATS:

        # Identify the video belonging to this snapshot
        source_videos : list = service.get_related(message['tenant'], message['deviceid'], message['timestamp'], 0, message['media_type'])

        # A snapshot should only ever have 2 max source recordings - interior and training, front is disabled
        if len(source_videos) <= 2:
            _logger.debug(f"Image {message['_id']} has {len(source_videos)} source videos: {source_videos}")

        sources = []
        if source_videos:
            for video in source_videos:
                sources.append(video)
        
        # Update snapshot record
        recording_item = {
            'video_id': message['_id'], # we have the key for snapshots named as 'video_id' due to legacy reasons...
            '_media_type': message['media_type'],
            'filepath': 's3://' + message['s3_path'],
            'recording_overview': {
                'tenantID': message['tenant'], 
                'deviceID': message['deviceid'],
                'source_videos': sources
            }
        }

        # Set reference to snapshots on source video
        for media_path in source_videos:            
            try:
                table_rec.update_one({'video_id': media_path}, {'$inc': {'recording_overview.#snapshots':1}, '$push': {'recording_overview.snapshots_paths':message['_id']}})
                _logger.info("Associated video to snapshot - %s" % media_path)
                update_voxel_media(table_rec.find_one({'video_id':media_path}))
            except Exception as e:
                _logger.exception("Unable to update snapshot information for id [%s] - %s" % (media_path, e))
        

    elif file_format in VIDEO_FORMATS:
        
        # Identify stored snapshots that belong to this recording
        snapshot_paths = service.get_related(message['tenant'], message['deviceid'], message['footagefrom'], message['footageto'], message['media_type'])
        snapshots_paths = []
        if snapshot_paths:
            for snapshot_path in snapshot_paths:
                snapshots_paths.append(snapshot_path)

        # Update video record
        recording_item = {
            'video_id': message['_id'],
            'MDF_available': message['MDF_available'],
            '_media_type': message['media_type'],
            'filepath': 's3://' + message['s3_path'],
            'recording_overview': {
                'tenantID': message['tenant'], 
                'deviceID': message['deviceid'],
                'length': message['length'],
                #'snapshots_paths': message['snapshots_paths'],
                'snapshots_paths': snapshots_paths,
                #'#snapshots': message['#snapshots'],
                '#snapshots': len(snapshot_paths),
                'time': datetime.fromtimestamp(message['footagefrom']/1000.0).strftime('%Y-%m-%d %H:%M:%S')
            },
            'resolution': message['resolution']
        }

        # Set reference to source video on snapshots
        for media_path in snapshot_paths:
            try: # we have the key for snapshots named as 'video_id' due to legacy reasons... 
                table_rec.update_one({'video_id': media_path}, {'$push': {'recording_overview.source_videos': message['_id']}})
                
                _logger.info("Associated snapshot to video - %s", media_path)
                # Create dataset with the bucket_name if it doesn't exist
                update_voxel_media(table_rec.find_one({'video_id':media_path}))
            except Exception:
                _logger.exception("Unable to upsert snapshot [%s]", media_path)

    # Upsert item into the 'recordings' collection, if the new item was built
    if recording_item:
        try:
            table_rec.update_one({'video_id': message["_id"]}, {'$set': recording_item}, upsert=True)
            _logger.info("Upserted recording DB item for item %s" % message["_id"])
            return recording_item
        except Exception as e:
            _logger.exception("Unable to create or replace recording item for item %s - %s" % (message["_id"], e))
    else:
        _logger.error(f"Unknown file format {file_format}")
    

def update_voxel_media(sample):
    
    def __get_s3_path(raw_path)->tuple[str, str]:
        print(raw_path) # s3://bucket/folder/ridecare_snapshot_1662080178308.jpeg
        import re
        match = re.match(r'^s3://([^/]+)/(.*)$', raw_path)
        
        if(match is None or len(match.groups()) != 2):
            raise ValueError('Invalid path: ' + raw_path)
        
        bucket = match.group(1)
        key = match.group(2)
        return bucket, key

    # Voxel51 code
    _ , filepath = __get_s3_path(sample["filepath"])
    s3split = filepath.split("/")
    bucket_name = s3split[0]
    filetype = s3split[-1].split(".")[-1]
    sample.pop("_id")

    if filetype in IMAGE_FORMATS:
        bucket_name = bucket_name+"_snapshots"
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+filepath[:-5]+'_anonymized.'+filetype
    elif filetype in VIDEO_FORMATS:
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+filepath[:-4]+'_anonymized.'+filetype
    else:
        raise ValueError("Unknown file format %s" % filetype)
    sample["s3_path"] = anonymized_path
    """
    {'video_id': 'ridecare_device_snapshot_1662080178308', '_id': ObjectId('6319c606d5e09ed78d407912'), '_media_type': 'image', 'filepath': 's3://bucket/folder/ridecare_snapshot_1662080178308.jpeg', 'recording_overview': {'tenantID': 'ridecare', 'deviceID': 'device', 'source_videos': ['ridecare_device_recording_1662080172308_1662080561893']}}
    3
    """
    try:
        # Create dataset with the bucket_name if it doesn't exist
        create_dataset(bucket_name)
        # Add  the video to the dataset
        update_sample(bucket_name, sample)
        # Create logs message
        _logger.info("Voxel sample [%s] created!", bucket_name)
    except Exception:
        _logger.exception("Unable to create dataset [%s] on update_pipeline_db !", bucket_name)


def upsert_mdf_data(message: dict, table_sig: Collection, table_rec: Collection) -> Optional[dict]:
    # verify message content
    if not ('signals_file' in message and 'bucket' in message['signals_file'] and 'key' in message['signals_file']):
        return None

    s3_client = boto3.client('s3', 'eu-central-1')
    signals_file_raw = ContainerServices.download_file(s3_client, message['signals_file']['bucket'], message['signals_file']['key'])
    signals = json.loads(signals_file_raw.decode('UTF-8'))
    try:
        # Upsert signals item
        mdf_data = {
            'recording_overview.number_chc_events': message['recording_overview']['number_chc_events'],
            'recording_overview.chc_duration': message['recording_overview']['chc_duration']
        }
        signals_data = {'source': "MDFParser",'signals': signals}
        table_rec.update_one({'video_id': message["_id"]}, {'$set': mdf_data}, upsert=True)
        table_sig.update_one({'recording': message["_id"], 'source': {'$regex':'MDF.*'}}, {'$set': signals_data}, upsert=True)
        # Create logs message
        _logger.info("Signals DB item [%s] upserted!", message["_id"])
    except Exception:
        _logger.exception("Unable to create or replace signals item for id: [%s]", message["_id"])

    return mdf_data


def update_pipeline_db(video_id: str, message: dict, table_pipe: Collection, source: str) -> Optional[dict]:
    """Inserts a new item (or updates it if already exists) on the
    pipeline execution collection
    Remark: Dictionaries and lists will be replaced instead of upserted. 

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
        _logger.error(
            'Skipping pipeline collection update for %s as not all required fields are present in the message.', video_id)
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
        upsert_item['processing_list'] = message['processing_steps']

    try:
        # Upsert pipeline executions item
        table_pipe.update_one({'_id': video_id}, {'$set': upsert_item}, upsert=True)
        # Create logs message
        _logger.info("Pipeline Exec DB item (Id: %s) updated!", video_id)
    except Exception:
        _logger.exception("Unable to create or replace pipeline executions item for id [%s]", video_id)

    # Voxel51 code
    s3split = message["s3_path"].split("/")
    bucket_name = s3split[0]

    filetype = s3split[-1].split(".")[-1]

    if filetype in IMAGE_FORMATS:
        bucket_name = bucket_name+"_snapshots"
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-5]+'_anonymized.'+filetype
    elif filetype in VIDEO_FORMATS:
        anonymized_path = "s3://"+os.environ['ANON_S3']+"/"+message["s3_path"][:-4]+'_anonymized.'+filetype
    else:
        raise ValueError("Unknown file format %s" % filetype)
    sample = upsert_item.copy() # with dicts either we make a copy or both variables will reference the same object
    sample.update({"video_id": video_id, "s3_path": anonymized_path})

    try:
        # Create dataset with the bucket_name if it doesn't exist
        create_dataset(bucket_name)
        # Add  the video to the dataset
        update_sample(bucket_name, sample)
        # Create logs message
        _logger.info("Voxel sample [%s] created!", bucket_name)
    except Exception:
        _logger.exception("Unable to create dataset [%s] on update_pipeline_db !", bucket_name)

    return upsert_item

def download_and_synchronize_chc(video_id: str, recordings_collection: Collection, bucket: str, key: str)->Tuple[dict, dict]:
        # get video length from the original recording entry
        recording_entry = recordings_collection.find_one({'video_id': video_id})
        if not('recording_overview' in recording_entry and 'length' in recording_entry['recording_overview']):
            raise ValueError('Recording entry for video_id %s does not have a length information.' % video_id)
        video_length = timedelta(seconds=pytimeparse.parse(recording_entry['recording_overview']['length']))
        
        # do the synchronisation
        chc_syncer = ChcSynchronizer()
        chc_dict = chc_syncer.download(bucket, key)
        chc_sync = chc_syncer.synchronize(chc_dict, video_length)
        chc_sync_parsed = {str(ts): signals for ts, signals in chc_sync.items()}

        # calculate CHC metrics
        chc_counter = ChcCounter()
        chc_metrics = chc_counter.process(chc_sync)

        return chc_sync_parsed, chc_metrics['recording_overview']

def process_outputs(video_id: str, message: dict, collection_algo_out: Collection, collection_recordings: Collection, collection_signals: Collection, source: str):
    """Inserts a new item on the algorithm output collection and, if
    there is a CHC file available for that item, processes
    that info and adds it to the item (plus updates the respective
    signals collection item with these signals)

    Arguments:
        message {dict} -- [info set from the received sqs message
                        that is used to populate the created item]
        collection_algo_out {MongoDB collection object} -- [Object used to
                                                    access/update the
                                                    algorithm output
                                                    collection information]
        collection_signals {MongoDB collection object} -- [Object used to
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
        _logger.error("Not able to create an entry because output or s3_path is missing!")
        return

    # Initialise variables used in item creation
    outputs = message['output']
    run_id = video_id + '_' + source
    output_paths = {}

    # Check if there is a metadata file path available
    if 'meta_path' in outputs and outputs['meta_path'] != "-":
        output_paths = {'metadata': f"{outputs['bucket']}/{outputs['meta_path']}"}

    # Check if there is a video file path available
    if "media_path" in outputs and outputs['media_path'] != "-":
        output_paths = {'video': f"{outputs['bucket']}/{outputs['media_path']}"}

    # Item creation (common parameters)
    algo_item: dict = {
        '_id': run_id,
        'algorithm_id': source,
        'pipeline_id': video_id,
        'output_paths': output_paths
    }

    # Compute results from CHC processing
    if source == "CHC" and 'meta_path' in outputs and 'bucket' in outputs:
        try:
            synchronized, chc_metrics = download_and_synchronize_chc(video_id, collection_recordings, outputs['bucket'], outputs['meta_path'])

            # Add video sync data processed from ivs_chain metadata file to created item
            algo_item.update({
                'results':{
                    'CHBs_sync': synchronized,
                    'CHC_metrics': chc_metrics
                }})
        
            signals_item = {
                        'algo_out_id': run_id,
                        'recording': video_id,
                        'source': "CHC",
                        'signals': synchronized,
                        }
            try:
                # upsert signals DB item
                sig_query = {'recording': video_id, 'algo_out_id': run_id}
                collection_signals.update_one(sig_query, {'$set': signals_item}, upsert=True)

                # Create logs message
                _logger.info("Signals DB item (algo id [%s]) updated!", run_id)

            except Exception:
                _logger.exception("Error updating the signals collection with CHC output [%s]", signals_item)
        except Exception:
            _logger.exception("Error synchronizing CHC output [%s]", outputs)

    try:
        # Insert previously built item
        #collection_algo_out.insert_one(algo_item,)
        query = {'_id':run_id}
        collection_algo_out.update_one(query, {'$set': algo_item}, upsert=True)
        _logger.info("Algo Output DB item (run_id: %s) created!", run_id)

    except Exception as e:
        _logger.exception(f"Error updating the algo output collection with item {algo_item} - {e}")

    # Voxel51 code
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
        sample["algorithms"][algo_item['_id']] = {
            "results":algo_item.get("results"), "output_paths":algo_item.get("output_paths")}
    else:
        sample["algorithms"][algo_item['_id']] = {"output_paths":algo_item.get("output_paths")}

    sample.pop('results')
    sample["s3_path"] = anon_video_path
    sample["video_id"] = algo_item["pipeline_id"]

    try:
        # Create dataset with the bucket_name if it doesn't exist
        create_dataset(bucket_name)

        # Add  the video to the dataset if it doesn't exist, otherwise update it
        update_sample(bucket_name, sample)

        # Create logs message
        _logger.info("Voxel sample [%s] created from process_outputs!", bucket_name)
    except Exception:
        _logger.exception("Unable to process Voxel entry [%s] on process_outputs!", bucket_name)


def upsert_data_to_db(db: Database, container_services: ContainerServices, service: RelatedMediaService, message: dict, message_attributes: dict):
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
        _logger.error('Skipping message due to neccessary content not being present.', message, message_attributes)
        return

    # Specify the tables to be used
    collection_signals = db[container_services.db_tables['signals']]
    collection_recordings = db[container_services.db_tables['recordings']]
    collection_pipeline_exec = db[container_services.db_tables['pipeline_exec']]
    collection_algo_output = db[container_services.db_tables['algo_output']]

    # Get source container name
    source = message_attributes['SourceContainer']['StringValue']

    #################### NOTE: Recording collection handling ##################

    # Voxel variables
    s3split = message["s3_path"].split("/")
    file_format = message["s3_path"].split(".")[-1]
    bucket_name = s3split[1] if file_format in VIDEO_FORMATS else s3split[1]+"_snapshots"
    name = message["s3_path"][message["s3_path"].find("/")+1:-4]
    if name[-1] == '.':
        name = name[0:-1]
    anon_s3_path = f"s3://{os.environ['ANON_S3']}/{name}_anonymized.{file_format}"

    # If the message is related to our data ingestion
    if source in {"SDRetriever", "MDFParser"}:
        recording_item = {'video_id': message['_id']} # default value for Voxel
        if file_format in IMAGE_FORMATS or file_format in VIDEO_FORMATS:
            # Call respective processing function
            recording_item = create_recording_item(message, collection_recordings, service)
            if not recording_item:
                # something went wrong when creating the new db record
                return

        elif source == "MDFParser":
            signals = upsert_mdf_data(message, collection_signals, collection_recordings)
            recording_item['signals'] = signals

        # Update Voxel with sample
        recording_item["s3_path"] = anon_s3_path
        try:
            # Create dataset with the bucket_name if it doesn't exist
            create_dataset(bucket_name)
            # Add the video to the dataset if it doesn't exist, otherwise update it
            update_sample(bucket_name, recording_item)
            # Create logs message
            _logger.info("Voxel sample [%s] updated from SDRetriever!", bucket_name)
        except Exception:
            _logger.exception("Unable to process Voxel entry [%s] on upsert_data_to_db!", bucket_name)
    
    # If the message is related to our data processing
    elif source in {"SDM", "Anonymize", "CHC", "anon_ivschain", "chc_ivschain"}:
   
        recording_id = os.path.basename(message["s3_path"]).split(".")[0]

        # Call respective processing function
        update_pipeline_db(recording_id, message, collection_pipeline_exec, source)

        # This should never be necessary, right? If we are processing a video, its record must exist, asserting just in case
        #assert collection_recordings.find_one({'video_id': recording_id})
        
        """ # DEBUG MANUAL UPLOADS (TODO: REMOVE AFTERWARDS)
        ##########################################################################################
        filetype = message["s3_path"].split(".")[-1]

        if filetype in IMAGE_FORMATS:
            media_type = 'image'
        elif filetype in IMAGE_FORMATS:
            media_type = 'video'
        
        # Check if item with that name already exists
        response_rec = collection_recordings.find_one({'video_id': recording_id})
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
            collection_recordings.insert_one(item_db)
            # Create logs message
            _logger.info("Recording DB empty item [%s] created with data [%s]", recording_id, item_db) """
                
        # Create/Update item on Algorithm Output DB if message is about algo output
        if 'output' in message:
            process_outputs(recording_id, message, collection_algo_output, collection_recordings, collection_signals, source)
    else:
        _logger.info(f"Unexpected message source {source} - {message}, {message_attributes}")


def read_message(container_services: ContainerServices, body: str)->dict:
    """Copies info received from other containers and
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

    _logger.info("Processing pipeline message..")

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
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
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

    # initialize service (to be removed, because it belongs to API)
    persistence = Persistence(None, container_services.db_tables, db_client.client)
    api_service = RelatedMediaService(persistence, s3_client)

    # use graceful exit
    graceful_exit = GracefulExit()

    _logger.info("Listening to input queue(s)..")

    while(graceful_exit.continue_running):
        # Check input SQS queue for new messages
        message = container_services.listen_to_input_queue(sqs_client)

        if message:
            _logger.info(message)
            # Processing step
            relay_list = read_message(container_services, message['Body'])

            # Insert/update data in db
            upsert_data_to_db(db_client, container_services, api_service, relay_list, message['MessageAttributes'])

            # Send message to output queue of metadata container
            output_queue = container_services.sqs_queues_list["Output"]
            container_services.send_message(sqs_client, output_queue, relay_list)

            # Delete message after processing
            container_services.delete_message(sqs_client, message['ReceiptHandle'])


if __name__ == '__main__':
    main()
