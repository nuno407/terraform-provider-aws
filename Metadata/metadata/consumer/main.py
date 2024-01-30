"""Metadata consumer entrypoint."""
import copy
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from json import JSONDecodeError
from logging import Logger
from typing import Optional, Tuple, Union
from urllib.parse import urlparse

import boto3
import pytimeparse
import pytz
from kink import inject
from mongoengine import connect, get_connection
from mypy_boto3_s3 import S3Client
from pymongo.collection import Collection, ReturnDocument
from pymongo.errors import DocumentTooLarge, PyMongoError

from base import GracefulExit
from base.aws.auto_message_visibility_increaser import \
    AutoMessageVisibilityIncreaser
from base.aws.container_services import ContainerServices, DATA_INGESTION_DATABASE_NAME
from base.aws.s3 import S3Controller
from base.aws.sqs import parse_message_body_to_dict
from base.chc_counter import ChcCounter
from base.constants import IMAGE_FORMATS, SIGNALS_FORMATS, VIDEO_FORMATS
from base.model.artifacts import (Artifact, EventArtifact, S3VideoArtifact,
                                  SignalsArtifact, SnapshotArtifact,
                                  VideoArtifact, parse_artifact, OperatorArtifact)
from base.model.artifacts.upload_rule_model import VideoUploadRule, SnapshotUploadRule
from metadata.common.constants import (AWS_REGION, TIME_FORMAT,
                                       UNKNOWN_FILE_FORMAT_MESSAGE)
from metadata.common.errors import (EmptyDocumentQueryResult,
                                    MalformedRecordingEntry)
from metadata.consumer.bootstrap import bootstrap_di
from metadata.consumer.chc_synchronizer import ChcSynchronizer
from metadata.consumer.exceptions import (NotSupportedArtifactError,
                                          SnapshotNotFound, IMUEmpty)
from metadata.consumer.imu_gap_finder import IMUGapFinder, TimeRange
from metadata.consumer.persistence import Persistence
from metadata.consumer.service import RelatedMediaService
from metadata.consumer.voxel.functions import (add_voxel_snapshot_metadata,
                                               update_on_voxel, update_rule_on_voxel)

CONTAINER_NAME = "Metadata"  # Name of the current container
CONTAINER_VERSION = "v6.3"   # Version of the current container
DOCUMENT_TOO_LARGE_MESSAGE = "Document too large %s"

_logger: Logger = ContainerServices.configure_logging("metadata")


@dataclass
class MetadataCollections:
    """"A wrapper for all metadata collections"""
    signals: Collection
    recordings: Collection
    pipeline_exec: Collection
    algo_output: Collection
    processed_imu: Collection
    events: Collection


def _get_anonymized_s3_path(file_key: str) -> str:
    filetype = file_key.split("/")[-1].split(".")[-1]

    if filetype not in IMAGE_FORMATS and filetype not in VIDEO_FORMATS:
        raise ValueError(UNKNOWN_FILE_FORMAT_MESSAGE % filetype)

    return f"s3://{os.environ['ANON_S3']}/{file_key.removesuffix(f'.{filetype}')}_anonymized.{filetype}"


def update_voxel_media(sample: dict):
    """
    Update data on a voxel sample.
    :param sample: Data to update. "s3_path" is used to search for the sample
    """
    # Voxel51 code

    if "filepath" not in sample:
        _logger.error("Filepath field not present in the sample. %s", sample)
        return
    _, file_key = S3Controller.get_s3_path_parts(sample["filepath"])

    sample.pop("_id", None)
    sample["s3_path"] = _get_anonymized_s3_path(file_key)
    sample["raw_filepath"] = sample["filepath"]

    update_on_voxel(sample["s3_path"], sample)


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
        "filepath": message["s3_path"],
        "recording_overview": {
            "tenantID": message["tenant"],
            "deviceID": message["deviceid"],
            "source_videos": list(related_media_paths),
            "devcloudid": message.get("devcloudid", None),
            "recording_time": datetime.fromtimestamp(message["timestamp"] / 1000.0, timezone.utc)
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

    recording_time = datetime.fromtimestamp(message["footagefrom"] / 1000.0, timezone.utc)
    footage_time = recording_time.strftime(TIME_FORMAT)

    # Update video record
    recording_item = {
        "video_id": message["_id"],
        "MDF_available": message["MDF_available"],
        "_media_type": message["media_type"],
        "filepath": message["s3_path"],
        "recording_overview": {
            "tenantID": message["tenant"],
            "deviceID": message["deviceid"],
            "length": message["length"],
            # Should replace `length` long term
            "recording_duration": message["recording_duration"],
            "snapshots_paths": related_media_paths,
            "#snapshots": len(related_media_paths),
            "time": footage_time,
            "recording_time": recording_time,  # Should replace `time` long term
            "devcloudid": message.get("devcloudid", None)
        },
        "resolution": message["resolution"]
    }

    if "imu_path" in message:
        recording_item["recording_overview"].update(
            {"imu_path": message["imu_path"]})

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
        recording_item = create_snapshot_recording_item(
            message, collection_rec, service)
    elif file_format in VIDEO_FORMATS:
        recording_item = create_video_recording_item(
            message, collection_rec, service)
    # Upsert item into the 'recordings' collection, if the new item was built
    if recording_item:
        try:
            update_query = transform_data_to_update_query(recording_item)
            recorder = collection_rec.find_one_and_update(filter={"video_id": message["_id"]},
                                                          update={
                                                              "$set": update_query},
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


def upsert_mdf_signals_data(
        message: dict, metadata_collections: MetadataCollections) -> Optional[dict]:
    """Upserts recording data and signals to respective collections.

    Args:
        message (dict): incoming SQS message
        collection_sig (Collection): mongodb signals collection
        collection_rec (Collection): mongodb recordings collection
    Returns:
        recording document (dict) from mongodb and parsed signals file downloaded from S3 (dict).
    """
    # verify message content
    if not ("recording_overview" in message and
            "parsed_file_path" in message):
        _logger.warning(
            "Expected fields for upserting MDF data are not present in the message.")
        return None

    bucket, key = S3Controller.get_s3_path_parts(message["parsed_file_path"])

    _logger.info("Using Endpoint %s", os.getenv("AWS_ENDPOINT", None))
    s3_client = boto3.client("s3", AWS_REGION, endpoint_url=os.getenv("AWS_ENDPOINT", None))
    signals_file_raw = ContainerServices.download_file(
        s3_client, bucket, key)
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

    # Upsert pipeline executions item
    table_pipe.update_one({"_id": video_id}, {
        "$set": upsert_item}, upsert=True)
    # Create logs message
    _logger.info("Pipeline Exec DB item (Id: %s) updated!", video_id)

    # Voxel51 code
    _, file_key = S3Controller.get_s3_path_parts(message["s3_path"])
    anonymized_path = _get_anonymized_s3_path(file_key)

    # with dicts either we make a copy or both variables will reference the same object
    sample = copy.deepcopy(upsert_item)
    sample.update({"video_id": video_id, "s3_path": anonymized_path,
                  "raw_filepath": message["s3_path"]})

    update_on_voxel(sample["s3_path"], sample)

    return upsert_item


def download_and_synchronize_chc(video_id: str, recordings_collection: Collection,
                                 bucket: str, key: str) -> Tuple[dict, dict]:
    """Downloads and synchronize CHC signals based on recording length.

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

    if not (
            "recording_overview" in recording_entry and "length" in recording_entry["recording_overview"]):
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


def process_outputs(video_id: str, message: dict,
                    metadata_collections: MetadataCollections, source: str):
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
            metadata_collections.signals.update_one(
                sig_query, {"$set": signals_item}, upsert=True)

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

    try:
        # Insert previously built item
        # collection_algo_out.insert_one(algo_item,)
        query = {"_id": run_id}
        metadata_collections.algo_output.update_one(
            query, {"$set": algo_item}, upsert=True)
        _logger.info("Algo Output DB item (run_id: %s) created!", run_id)
    except DocumentTooLarge as err:
        _logger.error(DOCUMENT_TOO_LARGE_MESSAGE, err)
        set_error_status(metadata_collections, video_id=video_id)
    except PyMongoError as err:
        _logger.exception(
            "Error updating the algo output collection with item %s - %s", algo_item, err)

    # Voxel51 code
    _, file_key = S3Controller.get_s3_path_parts(message["s3_path"])
    anon_video_path = _get_anonymized_s3_path(file_key)

    sample = copy.deepcopy(algo_item)

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
    sample["raw_filepath"] = message["s3_path"]
    sample["video_id"] = algo_item["pipeline_id"]

    update_on_voxel(sample["s3_path"], sample)


def set_error_status(metadata_collections: MetadataCollections, video_id: str) -> None:
    """Set recording data_status as error
    Args:
        video_id (str): recording id to modify the status
    """
    metadata_collections.pipeline_exec.find_one_and_update(
        filter={"video_id": video_id},
        update={"$set": {"data_status": "error"}}
    )


@inject
def insert_mdf_imu_data(imu_message: dict, metadata_collections: MetadataCollections,
                        s3_client: S3Client, imu_gap_finder: IMUGapFinder) -> tuple[list[TimeRange], str, str]:
    """ Receives a message from the MDF IMU queue, downloads IMU file from a S3 bucket
    and inserts into the timeseries database. Finally returns the start and end
    timestamp of that IMU data.

    The incoming message has the following format:
    {
        _id: <string>, # unique id for the message
        parsed_file_path: <string>, # S3 path to the parsed file
        data_type: <string>, # type of data (e.g. "imu", "metadata")
        recording_overview: <dict>, # recording overview
    }
    """
    _logger.debug("Inserting IMU data from message: %s", str(imu_message))
    # Get parsed file from S3
    file_path = imu_message["parsed_file_path"]
    parsed_path = urlparse(file_path)
    bucket_name = parsed_path.netloc
    object_key = parsed_path.path.strip("/")

    if not ContainerServices.check_s3_file_exists(s3_client, bucket_name, object_key):
        _logger.error(
            "The imu file (%s) is not available on the bucket (%s)", bucket_name, object_key)
        return [], "", ""

    raw_parsed_imu = ContainerServices.download_file(
        s3_client,
        bucket_name,
        object_key)

    # Load parsed file into DB
    parsed_imu = json.loads(raw_parsed_imu.decode("utf-8"))
    if len(parsed_imu) == 0:
        _logger.warning("The imu file (%s) does not contain any information", file_path)
        raise IMUEmpty

    for doc in parsed_imu:
        doc["timestamp"] = datetime.fromtimestamp(
            doc["timestamp"] / 1000, tz=pytz.utc)

    metadata_collections.processed_imu.insert_many(parsed_imu)
    _logger.info("IMU data was inserted into mongodb")
    return imu_gap_finder.get_valid_imu_time_ranges(
        parsed_imu), parsed_imu[0]["source"]["tenant"], parsed_imu[0]["source"]["device_id"]


def __update_events(imu_range: TimeRange, imu_tenant, imu_device, data_type: str,
                    metadata_collections: MetadataCollections) -> None:
    filter_query_events = {"$and": [
        {"last_shutdown.timestamp": {"$exists": False}},
        {"tenant_id": imu_tenant},
        {"device_id": imu_device},
        {"timestamp": {"$gte": imu_range.min}},
        {"timestamp": {"$lte": imu_range.max}},
    ]}

    filter_query_shutdowns = {"$and": [
        {"last_shutdown.timestamp": {"$exists": True}},
        {"last_shutdown.timestamp": {"$ne": None}},
        {"tenant_id": imu_tenant},
        {"device_id": imu_device},
        {"last_shutdown.timestamp": {"$gte": imu_range.min}},
        {"last_shutdown.timestamp": {"$lte": imu_range.max}},
    ]}

    update_query_events = {
        "$set": {
            data_type + "_available": True
        }
    }
    update_query_shutdowns = {
        "$set": {
            "last_shutdown." + data_type + "_available": True
        }
    }
    _logger.debug("Updating metadata events with filter=(%s) and update=(%s)",
                  str(filter_query_events), str(update_query_events))
    _logger.debug("Updating metadata shutdowns with filter=(%s) and update=(%s)",
                  str(filter_query_shutdowns), str(update_query_shutdowns))

    metadata_collections.events.update_many(filter=filter_query_events, update=update_query_events)
    metadata_collections.events.update_many(
        filter=filter_query_shutdowns,
        update=update_query_shutdowns)


def __process_mdfparser(message: dict, metadata_collections: MetadataCollections):

    del message["tenant"]
    del message["raw_s3_path"]

    if message["data_type"] == "imu":
        imu_ranges, imu_tenant, imu_device = insert_mdf_imu_data(
            message, metadata_collections)  # pylint: disable=no-value-for-parameter
        for imu_range in imu_ranges:
            __update_events(imu_range, imu_tenant, imu_device, "imu", metadata_collections)
        return

    recording = upsert_mdf_signals_data(
        message, metadata_collections)
    # ToDo: When refactoring signals to time series, update events as done with imu

    if not recording:
        # something went wrong when looking up the recording record
        _logger.warning(
            "Skip updating voxel, No recording item found on DB or IMU data.")
        return
    _logger.debug("Recording stored in DB: %s", str(recording))
    update_voxel_media(recording)


def __parse_sdr_message(artifact: Artifact) -> dict:
    """Parses a message coming from SDRetriever, which is expected to be an Artifact object."""
    # Fill common properties
    message: dict = {
        "_id": artifact.artifact_id,
        "s3_path": artifact.s3_path,
        "tenant": artifact.tenant_id,
        "deviceid": artifact.device_id,
        "devcloudid": artifact.devcloudid
    }

    # Fill specific properties
    if isinstance(artifact, VideoArtifact):
        length_td = timedelta(seconds=artifact.actual_duration)
        message["MDF_available"] = "No"
        message["length"] = str(length_td).split('.')[0]
        # Should replace 'length' long term
        message["recording_duration"] = artifact.actual_duration
        message["snapshots_paths"] = []
        message["media_type"] = "video"
        message["resolution"] = f"{artifact.resolution.width}x{artifact.resolution.height}"
        message["footagefrom"] = artifact.timestamp.timestamp() * 1000
        message["footageto"] = artifact.end_timestamp.timestamp() * 1000

    elif isinstance(artifact, SnapshotArtifact):
        message["timestamp"] = artifact.timestamp.timestamp() * 1000
        message["media_type"] = "image"

    elif isinstance(artifact, SignalsArtifact) and isinstance(artifact.referred_artifact, SnapshotArtifact):
        message["_id"] = artifact.referred_artifact.artifact_id

    else:
        _logger.info(
            "Artifact type %s is not supported by the current implementation.", type(artifact))
        raise NotSupportedArtifactError(
            f"Artifact type {str(type(artifact))} is not supported by the current implementation.")
    return message


def __process_sdr(message: dict, metadata_collections: MetadataCollections,
                  service: RelatedMediaService):
    """Process a message coming from SDRetriever."""

    artifact = parse_artifact(message)
    parsed_message = __parse_sdr_message(artifact)
    file_format = parsed_message.get("s3_path", "").split(".")[-1]

    if file_format in IMAGE_FORMATS or file_format in VIDEO_FORMATS:
        # Call respective processing function
        recording = create_recording_item(
            parsed_message, metadata_collections.recordings, service)
        if not recording:
            # something went wrong when creating the new db record
            _logger.warning("No recording item created on DB.")
            return
        update_voxel_media(recording)  # pylint: disable=no-value-for-parameter

    elif file_format in SIGNALS_FORMATS:
        if isinstance(artifact, SignalsArtifact) and isinstance(
                artifact.referred_artifact, SnapshotArtifact):
            add_voxel_snapshot_metadata(
                artifact)  # pylint: disable=no-value-for-parameter
        else:
            raise NotSupportedArtifactError(
                f"Artifact type {str(type(artifact))} is not supported by the current implementation.")
    else:
        _logger.error(
            "Unexpected file format %s from SDRetriever.", file_format)


def __convert_event_to_db_item(event: EventArtifact) -> dict:
    event_data = event.dict()
    return {k: v for (k, v) in event_data.items() if v is not None}


def process_sanitizer(message: dict, metadata_collections: MetadataCollections):
    """Process a message coming from Sanitizer."""
    artifact = parse_artifact(message)
    if isinstance(artifact, EventArtifact):
        event_collection = metadata_collections.events
        db_item = __convert_event_to_db_item(artifact)
        event_collection.insert_one(db_item)
    if isinstance(artifact, OperatorArtifact):
        # Workaround because DI is not initialized on import phase
        from metadata.consumer.database.operator_repository import OperatorRepository  # pylint: disable=import-outside-toplevel
        OperatorRepository.create_operator_feedback(artifact)


def __convert_video_rule_to_db_item(artifact: VideoUploadRule) -> dict:
    rule_data = {"name": artifact.rule.rule_name,
                 "version": artifact.rule.rule_version,
                 "origin": artifact.rule.origin,
                 "footage_from": artifact.footage_from,
                 "footage_to": artifact.footage_to}
    return rule_data


def __convert_snaphot_rule_to_db_item(artifact: SnapshotUploadRule) -> dict:
    rule_data = {"name": artifact.rule.rule_name,
                 "version": artifact.rule.rule_version,
                 "origin": artifact.rule.origin,
                 "snapshot_timestamp": artifact.snapshot_timestamp}
    return rule_data


def process_selector(artifact: Union[VideoUploadRule, SnapshotUploadRule], metadata_collections: MetadataCollections):
    """
    Process a message coming from Selector.
    """
    rule_collection = metadata_collections.recordings
    if isinstance(artifact, VideoUploadRule):
        db_item = __convert_video_rule_to_db_item(artifact)
        rule_collection.update_one(
            filter={
                "video_id": artifact.video_id,
                "_media_type": "video"
            },
            update={
                # in case that the video is not created we add the missing fields
                "$setOnInsert": {
                    "video_id": artifact.video_id,
                    "_media_type": "video"
                },
                # Add rule to upload_rule list
                "$addToSet": {"upload_rules": db_item}
            },
            upsert=True
        )
        update_rule_on_voxel(artifact.raw_file_path, artifact.video_id, artifact.tenant, db_item)
    elif isinstance(artifact, SnapshotUploadRule):
        db_item = __convert_snaphot_rule_to_db_item(artifact)
        rule_collection.update_one(
            filter={
                "video_id": artifact.snapshot_id,
                "_media_type": "image"
            },
            update={
                # in case that the video is not created we add the missing fields
                "$setOnInsert": {
                    "video_id": artifact.snapshot_id,
                    "_media_type": "image"
                },
                # Add rule to upload_rule list
                "$addToSet": {"upload_rules": db_item}
            },
            upsert=True
        )
        update_rule_on_voxel(artifact.raw_file_path, artifact.snapshot_id, artifact.tenant, db_item)


def __process_general(message: dict, metadata_collections: MetadataCollections, source: str):
    recording_id = os.path.basename(message["s3_path"]).split(".")[0]

    # Call respective processing function
    update_pipeline_db(recording_id, message,
                       metadata_collections.pipeline_exec, source)

    # Create/Update item on Algorithm Output DB if message is about algo output
    if "output" in message:
        process_outputs(recording_id, message, metadata_collections, source)


def upsert_data_to_db(service: RelatedMediaService,
                      message: dict,
                      message_attributes: dict,
                      metadata_collections: MetadataCollections):
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
            "Skipping message due to necessary content not being present. Message: %s  Attributes: %s",
            message,
            message_attributes)
        return

    # Get source container name
    source = message_attributes["SourceContainer"]["StringValue"]

    #################### NOTE: Recording collection handling ##################
    # If the message is related to our data ingestion
    if source == "SDRetriever":
        try:
            __process_sdr(message, metadata_collections, service)
        except NotSupportedArtifactError:
            _logger.info("Skipping not supported artifact")
    elif source == "MDFParser":
        __process_mdfparser(message, metadata_collections)
    elif source in {"SDM", "Anonymize", "CHC", "anon_ivschain", "chc_ivschain"}:
        __process_general(message, metadata_collections, source)
    else:
        _logger.info("Unexpected message source %s - %s, %s",
                     source, message, message_attributes)


def fix_message(container_services: ContainerServices, body: str, dict_body: dict) -> dict:
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

    if "data_type" in body and "parsed_file_path" in dict_body:
        s3_path = dict_body["parsed_file_path"]
    else:
        s3_path = dict_body.get("s3_path", "s3://" + dict_body.get("signals_file", dict()).get(
            "bucket", "") + "/" + dict_body.get("signals_file", dict()).get("key", ""))
    container_services.display_processed_msg(s3_path)

    # there are services that send a s3_key instead of an s3_path!
    # There are services that send a s3_bucket/s3_key without s3://
    # TODO: Fix services
    if "s3_path" in dict_body:
        s3_path = str(dict_body["s3_path"])
        if s3_path.find("s3://") == -1:
            if s3_path.find("raw-video-files") != -1:
                dict_body["s3_path"] = f"s3://{s3_path}"
            else:
                dict_body["s3_path"] = f"s3://{os.environ['RAW_S3']}{s3_path if s3_path[0] == '/' else '/' + s3_path }"
        dict_body["filepath"] = dict_body["s3_path"]
    return dict_body


def main():
    """Main function"""
    # Define configuration for logging messages
    _logger.info("Starting Container %s (%s)..\n", CONTAINER_NAME, CONTAINER_VERSION)

    bootstrap_di()

    # Create the necessary clients for AWS services access
    sqs_client = boto3.client(
        'sqs',
        region_name=AWS_REGION,
        endpoint_url=os.getenv(
            "AWS_ENDPOINT",
            None))

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME, version=CONTAINER_VERSION)

    # Load global variable values from yaml config
    container_services.load_config_vars()
    container_services.load_mongodb_config_vars()
    os.environ["ANON_S3"] = container_services.anonymized_s3
    os.environ["RAW_S3"] = container_services.raw_s3

    # initialize DB client
    db_client = container_services.create_db_client()
    host = os.environ["FIFTYONE_DATABASE_URI"]
    connect(db=DATA_INGESTION_DATABASE_NAME, host=host, alias="DataIngestionDB")

    # initialize service (to be removed, because it belongs to API)
    persistence = Persistence(container_services.db_tables, db_client.client)
    api_service = RelatedMediaService(persistence)

    # use graceful exit
    graceful_exit = GracefulExit()

    _logger.info("Listening to input queue(s)..")

    while graceful_exit.continue_running:
        # Check input SQS queue for new messages
        message = container_services.get_single_message_from_input_queue(sqs_client)

        # Convert message from string to dict

        if message and "Body" in message:
            message_dict = parse_message_body_to_dict(message["Body"])
            if "messageAttributes" in message_dict and "body" in message_dict:
                _logger.info("Message generated from sqs to sns subscription.")
                message["MessageAttributes"] = message_dict["messageAttributes"]
                message["Body"] = message_dict["body"]

                # Capitilizes the message attribute type keys
                for attr_key, attr_dict in message["MessageAttributes"].items():
                    new_dict = {}
                    for attr_type, attrt_value in attr_dict.items():
                        cap_attr_type = attr_type[0].upper() + attr_type[1:]
                        new_dict[cap_attr_type] = attrt_value

                    message["MessageAttributes"][attr_key] = new_dict

        if message and "MessageAttributes" not in message:
            _logger.error(
                "Message received without MessageAttributes, going to delete it: %s",
                message)
            container_services.delete_message(sqs_client, message["ReceiptHandle"])
            continue

        if message:
            _logger.info("Processing pipeline message..")
            _logger.info(message)
            # Convert message from string to dict
            message_dict = parse_message_body_to_dict(message["Body"])
            # Processing step to be compatible with whatever is done later
            fixed_message_dict = fix_message(
                container_services, message["Body"], message_dict.copy())
            # Get source container name
            source = message.get(
                "MessageAttributes",
                {}).get(
                "SourceContainer",
                {}).get(
                "StringValue",
                None)

            # Get metadata collection names
            metadata_collections = MetadataCollections(
                signals=db_client[container_services.db_tables["signals"]],
                recordings=db_client[container_services.db_tables["recordings"]],
                pipeline_exec=db_client[container_services.db_tables["pipeline_exec"]],
                algo_output=db_client[container_services.db_tables["algo_output"]],
                processed_imu=db_client[container_services.db_tables["processed_imu"]],
                events=db_client[container_services.db_tables["events"]]
            )

            # Insert/update data in db

            try:
                with AutoMessageVisibilityIncreaser(sqs_client, message["ReceiptHandle"],
                                                    container_services, 60, container_services.input_queue):
                    if source == "Sanitizer":
                        process_sanitizer(message_dict, metadata_collections)
                    elif source == "Selector":
                        if message_dict.get("artifact_name", None) == "video_rule":
                            process_selector(VideoUploadRule(**message_dict), metadata_collections)
                        elif message_dict.get("artifact_name", None) == "snapshot_rule":
                            process_selector(SnapshotUploadRule(**message_dict), metadata_collections)
                        else:
                            raise NotSupportedArtifactError(
                                f"Received message from selector with wrong artifact_name. Message {message_dict}")
                    else:
                        upsert_data_to_db(api_service,
                                          fixed_message_dict,
                                          message["MessageAttributes"],
                                          metadata_collections)
            except SnapshotNotFound:
                _logger.warning(
                    "The referred snapshot was not found, it will be reingested later")
                continue
            except JSONDecodeError as err:
                _logger.error("Malformed json file received %s", err)
                continue

            # Delete message after processing
            container_services.delete_message(
                sqs_client, message["ReceiptHandle"])


if __name__ == "__main__":
    main()
