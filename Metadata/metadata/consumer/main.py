"""Metadata consumer entrypoint."""
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Optional, Tuple

import boto3
import pytimeparse
import pytz
from kink import inject
from pymongo.collection import Collection, ReturnDocument
from pymongo.database import Database
from pymongo.errors import DocumentTooLarge, PyMongoError

from base import GracefulExit
from base.aws.container_services import ContainerServices
from base.chc_counter import ChcCounter
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
from base.voxel.voxel_functions import create_dataset, update_sample

from metadata.common.constants import (AWS_REGION, TIME_FORMAT, UNKNOWN_FILE_FORMAT_MESSAGE)
from metadata.common.errors import (EmptyDocumentQueryResult, MalformedRecordingEntry)
from metadata.consumer.bootstrap import bootstrap_di
from metadata.consumer.chc_synchronizer import ChcSynchronizer
from metadata.consumer.config import DatasetMappingConfig
from metadata.consumer.db import Persistence
from metadata.consumer.service import RelatedMediaService

CONTAINER_NAME = "Metadata"  # Name of the current container
CONTAINER_VERSION = "v6.2"   # Version of the current container
DOCUMENT_TOO_LARGE_MESSAGE = "Document too large %s"

_logger: Logger = ContainerServices.configure_logging("metadata")


@dataclass
class MetadataCollections:
    """"A wrapper for all metadata collections"""
    signals: Collection
    recordings: Collection
    pipeline_exec: Collection
    algo_output: Collection


def __get_s3_path_parts(raw_path) -> Tuple[str, str]:
    match = re.match(r"^s3://([^/]+)/(.*)$", raw_path)

    if match is None or len(match.groups()) != 2:
        raise ValueError("Invalid path: " + raw_path)

    bucket = match.group(1)
    key = match.group(2)
    return bucket, key


def _get_anonymized_s3_path(filepath):
    filetype = filepath.split("/")[-1].split(".")[-1]

    if filetype not in IMAGE_FORMATS and filetype not in VIDEO_FORMATS:
        raise ValueError(UNKNOWN_FILE_FORMAT_MESSAGE % filetype)

    return f"s3://{os.environ['ANON_S3']}/{filepath.rstrip(f'.{filetype}')}_anonymized.{filetype}"


def _update_on_voxel(filepath: str, sample: dict):
    """
    Updates a sample in a dataset with the given metadata. If the sample or dataset do not exist they will be created.
    The dataset name is derived by the given S3 file path.
    From the path the tenant is derived and the dataset determined.
    :param filepath: File path to extract the dataset information from.
    :param sample: Sample data to update. Uses `video_id` to find the sample.
    """
    dataset_name, tags = _determine_dataset_name(filepath)
    try:
        create_dataset(dataset_name, tags)
        update_sample(dataset_name, sample)
    except Exception as err:  # pylint: disable=broad-except
        _logger.exception("Unable to process Voxel entry [%s] with %s", dataset_name, err)


@inject
def _determine_dataset_name(filepath: str, mapping_config: DatasetMappingConfig):
    """
    Checks in config if tenant gets its own dataset or if it is part of the default dataset.
    Dedicated dataset names are prefixed with the tag given in the config.
    The tag is not added to the default dataset.

    :param filepath: S3 filepath to extract the tenant from
    :param mapping_config: Config with mapping information about the tenants
    :return: the resulting dataset name and the tags which should be added on dataset creation
    """
    s3split = filepath.split("/")
    # The root dir on the S3 bucket always is the tenant name
    tenant_name = s3split[0]
    filetype = s3split[-1].split(".")[-1]

    dataset_name = mapping_config.default_dataset
    tags = [mapping_config.tag]

    if tenant_name in mapping_config.create_dataset_for:
        dataset_name = f"{mapping_config.tag}-{tenant_name}"

    if filetype in IMAGE_FORMATS:
        dataset_name = dataset_name + "_snapshots"

    return dataset_name, tags


def update_voxel_media(sample: dict):
    """
    Update data on a voxel sample.
    :param sample: Data to update. "s3_path" is used to search for the sample
    """
    # Voxel51 code

    if "filepath" not in sample:
        _logger.error("Filepath field not present in the sample. %s", sample)
        return

    _, filepath = __get_s3_path_parts(sample["filepath"])

    sample.pop("_id", None)
    sample["s3_path"] = _get_anonymized_s3_path(filepath)

    _update_on_voxel(filepath, sample)


def find_and_update_media_references(
        related_media_paths: list[str],
        update_query: dict,
        recordings_collection: Collection) -> None:
    """find_and_update_media_references.

    Find and update reference in documents between video and snaphots

    Args:
        related_media_paths (list[str]): collection of media that is associated
        update_query (dict): update query
        recordings_collection (Collection): recordings mongodb collection
    """

    for media_path in related_media_paths:
        try:
            # we have the key for snapshots named as 'video_id' due to legacy reasons...
            recording = recordings_collection.find_one_and_update(
                filter={"video_id": media_path},
                update=update_query,
                upsert=False,
                return_document=ReturnDocument.AFTER)

            _logger.info("Associated snapshot to video - %s", media_path)
            update_voxel_media(recording)
        except PyMongoError as err:
            _logger.exception(
                "Unable to upsert snapshot [%s] :: %s", media_path, err)


def create_snapshot_recording_item(message: dict, collection_rec: Collection,
                                   service: RelatedMediaService) -> dict:
    """create_snapshot_recording_item.

    Args:
        message (dict): info set from the sqs message received that will be used to populate the newly
                        created DB item
        collection_rec (MongoDB collection object): Object used to access/update the recordings collection
                                                    information
        service (RelatedMediaService): reponsible for finding related video for a given snapshot
    Returns:
        recording_item (dict): snapshot recording item document
    """
    related_media_paths: list = service.get_related(
        tenant=message["tenant"],
        deviceid=message["deviceid"],
        start=message["timestamp"],
        end=0,
        media_type=message["media_type"])

    # Update snapshot record
    recording_item = {
        # we have the key for snapshots named as "video_id" due to legacy reasons...
        "video_id": message["_id"],
        "_media_type": message["media_type"],
        "filepath": "s3://" + message["s3_path"],
        "recording_overview": {
            "tenantID": message["tenant"],
            "deviceID": message["deviceid"],
            "source_videos": list(related_media_paths),
            "internal_message_reference_id": message.get("internal_message_reference_id", None)
        }
    }

    recording_entry_result = collection_rec.find_one(
        {"video_id": message["_id"]})

    if recording_entry_result:
        _logger.warning(
            "The snapshot already exists in the database, will not be appended to the correlated references")  # pylint: disable=line-too-long
        return recording_item

    find_and_update_media_references(related_media_paths, update_query={
        "$inc": {"recording_overview.#snapshots": 1},
        "$push": {
            "recording_overview.snapshots_paths": message["_id"]
        }
    }, recordings_collection=collection_rec)

    return recording_item


def create_video_recording_item(message: dict, collection_rec: Collection,
                                service: RelatedMediaService) -> dict:
    """create_video_recording_item.

    Args:
        message (dict): info set from the sqs message received that will be used to populate the newly
                        created DB item
        collection_rec (MongoDB collection object): Object used to access/update the recordings collection
                                                    information
        service (RelatedMediaService): reponsible for finding related video for a given snapshot
    Returns:
        recording_item (dict): snapshot recording item document
    """
    related_media_paths: list = service.get_related(
        tenant=message["tenant"],
        deviceid=message["deviceid"],
        start=message["footagefrom"],
        end=message["footageto"],
        media_type=message["media_type"])

    footage_time = datetime.fromtimestamp(
        message["footagefrom"] / 1000.0, timezone.utc).strftime(TIME_FORMAT)

    # Update video record
    recording_item = {
        "video_id": message["_id"],
        "MDF_available": message["MDF_available"],
        "_media_type": message["media_type"],
        "filepath": "s3://" + message["s3_path"],
        "recording_overview": {
            "tenantID": message["tenant"],
            "deviceID": message["deviceid"],
            "length": message["length"],
            "snapshots_paths": related_media_paths,
            "#snapshots": len(related_media_paths),
            "time": footage_time,
            "internal_message_reference_id": message.get("internal_message_reference_id", None)
        },
        "resolution": message["resolution"]
    }

    find_and_update_media_references(related_media_paths, update_query={
        "$push": {
            "recording_overview.source_videos": message["_id"]
        }
    }, recordings_collection=collection_rec)

    return recording_item


def create_recording_item(message: dict,
                          collection_rec: Collection, service: RelatedMediaService) -> Optional[dict]:
    """Inserts a new item on the recordings collection

    Arguments:
        message (dict): info set from the sqs message received that will be used to populate the newly
                        created DB item
        collection_rec (MongoDB collection object): Object used to access/update the recordings collection
                                                    information
        service (RelatedMediaService): reponsible for finding related video for a given snapshot
    """

    # check that data has at least the absolute minimum of required fields
    if not all([key in message for key in ["_id", "s3_path"]]):
        raise ValueError("Invalid Message")

    # Build item structure and add info from msg received
    file_format = message["s3_path"].split(".")[-1]
    recording_item = {}

    if file_format in IMAGE_FORMATS:
        recording_item = create_snapshot_recording_item(message, collection_rec, service)
    elif file_format in VIDEO_FORMATS:
        recording_item = create_video_recording_item(message, collection_rec, service)
    # Upsert item into the 'recordings' collection, if the new item was built
    if recording_item:
        try:
            update_query = transform_data_to_update_query(recording_item)
            recorder = collection_rec.find_one_and_update(filter={"video_id": message["_id"]},
                                                          update={"$set": update_query},
                                                          upsert=True, return_document=ReturnDocument.AFTER)

            _logger.info("Upserted recording DB item for item %s",
                         message["_id"])
            _logger.debug("Recording upsert in DB %s", str(recorder))
        except PyMongoError as err:
            _logger.exception(
                "Unable to create or replace recording item for item %s - %s", message["_id"], err)
        return recording_item

    _logger.error(UNKNOWN_FILE_FORMAT_MESSAGE, file_format)
    return None


def transform_data_to_update_query(data: dict) -> dict:
    """Transforms nested dict data to mongodb update statement."""
    update_query = {}
    for key, value in data.items():
        if isinstance(value, dict):
            nested_data = transform_data_to_update_query(value)
            for nested_key, nested_value in nested_data.items():
                update_query[f"{key}.{nested_key}"] = nested_value
        elif value is not None:
            update_query[key] = value
    return update_query


def upsert_mdf_data(message: dict, metadata_collections: MetadataCollections) -> Optional[dict]:
    """Upserts recording data and signals to respective collections.

    Args:
        message (dict): incoming SQS message
        collection_sig (Collection): mongodb signals collection
        collection_rec (Collection): mongodb recordings collection
    Returns:
        recording document (dict) from mongodb and parsed signals file downloaded from S3 (dict).
    """
    # verify message content
    if not ("signals_file" in message and
            "bucket" in message["signals_file"] and
            "key" in message["signals_file"]):
        _logger.warning(
            "Expected fields for upserting MDF data are not present in the message.")
        return None

    s3_client = boto3.client("s3", AWS_REGION)
    signals_file_raw = ContainerServices.download_file(
        s3_client, message["signals_file"]["bucket"], message["signals_file"]["key"])
    signals = json.loads(signals_file_raw.decode("UTF-8"))

    try:
        # Upsert signals item
        aggregated_values = {}
        for key in message["recording_overview"].keys():
            aggregated_values.update(
                {f"recording_overview.{key}": message["recording_overview"][key]})

        signals_data = {"source": "MDFParser", "signals": signals}

        recording = metadata_collections.recordings.find_one_and_update(
            filter={"video_id": message["_id"]},
            update={"$set": aggregated_values},
            upsert=True,
            return_document=ReturnDocument.AFTER)

        metadata_collections.signals.update_one(
            filter={
                "recording": message["_id"],
                "source": {"$regex": "MDF.*"}
            },
            update={"$set": signals_data},
            upsert=True)

        # Create logs message
        _logger.info("Signals DB item [%s] upserted!", message["_id"])
        return recording
    except DocumentTooLarge as err:
        _logger.warning("Document too large %s", err)
        set_error_status(metadata_collections, video_id=message["_id"])
    except PyMongoError as err:
        _logger.exception(
            "Unable to create or replace signals item for id: [%s] :: %s", message["_id"], err)
    return None


def update_pipeline_db(video_id: str, message: dict,
                       table_pipe: Collection, source: str) -> Optional[dict]:
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
    if not all(k in message for k in ("data_status", "s3_path")):
        _logger.error(
            "Skipping pipeline collection update for %s as not all required fields are present in the message.",
            video_id)
        return None

    # Initialise pipeline item to upsert
    timestamp = str(datetime.now(
        tz=pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

    upsert_item = {
        "_id": video_id,
        "data_status": message["data_status"],
        "from_container": CONTAINER_NAME,
        "info_source": source,
        "last_updated": timestamp,
        "s3_path": message["s3_path"]
    }

    if "processing_steps" in message:
        upsert_item["processing_list"] = message["processing_steps"]

    try:
        # Upsert pipeline executions item
        table_pipe.update_one({"_id": video_id}, {
                              "$set": upsert_item}, upsert=True)
        # Create logs message
        _logger.info("Pipeline Exec DB item (Id: %s) updated!", video_id)
    except PyMongoError as err:
        _logger.exception(
            "Unable to create or replace pipeline executions item for id [%s] :: %s", video_id, err)

    # Voxel51 code
    filepath = message["s3_path"]
    anonymized_path = _get_anonymized_s3_path(filepath)

    # with dicts either we make a copy or both variables will reference the same object
    sample = upsert_item.copy()
    sample.update({"video_id": video_id, "s3_path": anonymized_path})

    _update_on_voxel(filepath, sample)

    return upsert_item


def download_and_synchronize_chc(video_id: str, recordings_collection: Collection,
                                 bucket: str, key: str) -> Tuple[dict, dict]:
    """Downloads and synchronize CHC signals based on kinesis recording length.

    Args:
        video_id (str): video identifier.
        recordings_collection (Collection): collection of recordings
        bucket (str): S3 bucket name
        key (str): S3 object key
    Returns:
        Parsed CHC signals and recording overview information.
    """
    # get video length from the original recording entry
    recording_entry_result = recordings_collection.find_one(
        {"video_id": video_id})
    if not recording_entry_result:
        raise EmptyDocumentQueryResult(
            f"Empty recording_entry result for video_id: {video_id}")

    assert recording_entry_result is not None
    recording_entry = recording_entry_result

    if not ("recording_overview" in recording_entry and "length" in recording_entry["recording_overview"]):
        raise MalformedRecordingEntry(
            f"Recording entry for video_id {video_id} does not have a length information.")
    video_length = timedelta(seconds=pytimeparse.parse(
        recording_entry["recording_overview"]["length"]))

    # do the synchronisation
    chc_syncer = ChcSynchronizer()
    chc_dict = chc_syncer.download(bucket, key)
    chc_sync = chc_syncer.synchronize(chc_dict, video_length)
    chc_sync_parsed = {str(ts): signals for ts,
                       signals in chc_sync.items()}

    # calculate CHC metrics
    chc_counter = ChcCounter()
    chc_metrics = chc_counter.process(chc_sync)

    return chc_sync_parsed, chc_metrics["recording_overview"]


def process_outputs(video_id: str, message: dict, metadata_collections: MetadataCollections, source: str):
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
    if not (all(k in message for k in ("output", "s3_path")) and "bucket" in message["output"]):
        _logger.error(
            "Not able to create an entry because output or s3_path is missing!")
        return

    # Initialise variables used in item creation
    outputs = message["output"]
    run_id = video_id + "_" + source
    output_paths = {}

    # Check if there is a metadata file path available
    if "meta_path" in outputs and outputs["meta_path"] != "-":
        output_paths = {
            "metadata": f"{outputs['bucket']}/{outputs['meta_path']}"}

    # Check if there is a video file path available
    if "media_path" in outputs and outputs["media_path"] != "-":
        output_paths = {
            "video": f"{outputs['bucket']}/{outputs['media_path']}"}

    # Item creation (common parameters)
    algo_item: dict = {
        "_id": run_id,
        "algorithm_id": source,
        "pipeline_id": video_id,
        "output_paths": output_paths
    }

    # Compute results from CHC processing
    if source == "CHC" and "meta_path" in outputs and "bucket" in outputs:
        try:
            synchronized, chc_metrics = download_and_synchronize_chc(
                video_id,
                metadata_collections.recordings,
                outputs["bucket"],
                outputs["meta_path"])

            # Add video sync data processed from ivs_chain metadata file to created item
            algo_item.update({
                "results": {
                    "CHBs_sync": synchronized,
                    "CHC_metrics": chc_metrics
                }})

            signals_item = {
                "algo_out_id": run_id,
                "recording": video_id,
                "source": "CHC",
                "signals": synchronized,
            }
            # upsert signals DB item
            sig_query = {"recording": video_id, "algo_out_id": run_id}
            metadata_collections.signals.update_one(sig_query, {"$set": signals_item}, upsert=True)

            # Create logs message
            _logger.info("Signals DB item (algo id [%s]) updated!", run_id)

        except DocumentTooLarge as err:
            _logger.error(DOCUMENT_TOO_LARGE_MESSAGE, err)
            set_error_status(metadata_collections, video_id=video_id)
        except PyMongoError:
            _logger.exception(
                "Error updating the signals collection with CHC output [%s]", signals_item)
        except EmptyDocumentQueryResult as err:
            _logger.exception("Recording with video_id not found %s", err)
        except MalformedRecordingEntry as err:
            _logger.exception("Error in recording overview %s", err)
        except Exception:  # pylint: disable=broad-except
            _logger.exception("Error synchronizing CHC output [%s]", outputs)

    try:
        # Insert previously built item
        # collection_algo_out.insert_one(algo_item,)
        query = {"_id": run_id}
        metadata_collections.algo_output.update_one(query, {"$set": algo_item}, upsert=True)
        _logger.info("Algo Output DB item (run_id: %s) created!", run_id)
    except DocumentTooLarge as err:
        _logger.error(DOCUMENT_TOO_LARGE_MESSAGE, err)
        set_error_status(metadata_collections, video_id=video_id)
    except PyMongoError as err:
        _logger.exception(
            "Error updating the algo output collection with item %s - %s", algo_item, err)

    # Voxel51 code
    filepath = message["s3_path"]
    anon_video_path = _get_anonymized_s3_path(filepath)

    sample = algo_item

    sample["algorithms"] = {}

    if source == "CHC":
        sample["algorithms"][algo_item["_id"]] = {
            "results": algo_item.get("results"), "output_paths": algo_item.get("output_paths")}
    else:
        sample["algorithms"][algo_item["_id"]] = {
            "output_paths": algo_item.get("output_paths")}

    if "results" in sample:
        sample.pop("results")

    sample["s3_path"] = anon_video_path
    sample["video_id"] = algo_item["pipeline_id"]

    _update_on_voxel(filepath, sample)


def set_error_status(metadata_collections: MetadataCollections, video_id: str) -> None:
    """Set recording data_status as error
    Args:
        video_id (str): recording id to modify the status
    """
    metadata_collections.pipeline_exec.find_one_and_update(
        filter={"video_id": video_id},
        update={"$set": {"data_status": "error"}}
    )


def upsert_data_to_db(db: Database, container_services: ContainerServices,
                      service: RelatedMediaService, message: dict, message_attributes: dict):
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
    if not ("SourceContainer" in message_attributes and
            "StringValue" in message_attributes["SourceContainer"]):
        _logger.error(
            "Skipping message due to neccessary content not being present. Message: %s  Atributes: %s",
            message,
            message_attributes)
        return

    metadata_collections = MetadataCollections(
        signals=db[container_services.db_tables["signals"]],
        recordings=db[container_services.db_tables["recordings"]],
        pipeline_exec=db[container_services.db_tables["pipeline_exec"]],
        algo_output=db[container_services.db_tables["algo_output"]]
    )

    # Get source container name
    source = message_attributes["SourceContainer"]["StringValue"]

    #################### NOTE: Recording collection handling ##################
    # If the message is related to our data ingestion
    if source == "SDRetriever":
        file_format = message.get("s3_path", "").split(".")[-1]
        if file_format in IMAGE_FORMATS or file_format in VIDEO_FORMATS:
            # Call respective processing function
            recording = create_recording_item(
                message, metadata_collections.recordings, service)
            if not recording:
                # something went wrong when creating the new db record
                _logger.warning("No recording item created on DB.")
                return
            update_voxel_media(recording)
        else:
            _logger.warning(
                "Unexpected file format %s from SDRetriever.", file_format)
    elif source == "MDFParser":
        recording = upsert_mdf_data(
            message, metadata_collections)

        _logger.debug("Recording stored in DB: %s", str(recording))
        if not recording:
            # something went wrong when looking up the recording record
            _logger.warning("No recording item found on DB.")
            return
        update_voxel_media(recording)

    # If the message is related to our data processing
    elif source in {"SDM", "Anonymize", "CHC", "anon_ivschain", "chc_ivschain"}:

        recording_id = os.path.basename(message["s3_path"]).split(".")[0]

        # Call respective processing function
        update_pipeline_db(recording_id, message, metadata_collections.pipeline_exec, source)

        # Create/Update item on Algorithm Output DB if message is about algo output
        if "output" in message:
            process_outputs(recording_id, message, metadata_collections, source)
    else:
        _logger.info("Unexpected message source %s - %s, %s",
                     source, message, message_attributes)


def read_message(container_services: ContainerServices, body: str) -> dict:
    """Copies info received from other containers and
    converts it from string into a dictionary

    Arguments:
        container_services {base.aws.container_services.ContainerServices}
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
    relay_data: dict = dict_body

    s3_path = relay_data.get("s3_path", "s3://" + relay_data.get("signals_file", dict()).get(
        "bucket", "") + "/" + relay_data.get("signals_file", dict()).get("key", ""))
    container_services.display_processed_msg(s3_path)

    return relay_data


def main():
    """Main function"""
    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
                 CONTAINER_VERSION)

    bootstrap_di()

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client('sqs',
                              region_name=AWS_REGION)

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from yaml config
    container_services.load_config_vars()
    container_services.load_mongodb_config_vars()
    os.environ["ANON_S3"] = container_services.anonymized_s3

    # initialize DB client
    db_client = container_services.create_db_client()

    # initialize service (to be removed, because it belongs to API)
    persistence = Persistence(
        None, container_services.db_tables, db_client.client)
    api_service = RelatedMediaService(persistence)

    # use graceful exit
    graceful_exit = GracefulExit()

    _logger.info("Listening to input queue(s)..")

    while graceful_exit.continue_running:
        # Check input SQS queue for new messages
        message = container_services.get_single_message_from_input_queue(sqs_client)

        if message:
            _logger.info(message)
            # Processing step
            relay_list = read_message(container_services, message["Body"])
            # Insert/update data in db
            upsert_data_to_db(db_client, container_services,
                              api_service, relay_list, message["MessageAttributes"])

            # Delete message after processing
            container_services.delete_message(
                sqs_client, message["ReceiptHandle"])


if __name__ == "__main__":
    main()
