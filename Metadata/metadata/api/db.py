"""Metadata api database controller."""
import copy
from math import ceil
from typing import Dict

from pymongo.mongo_client import MongoClient

from base.aws.container_services import DATA_INGESTION_DATABASE_NAME


class Persistence:  # pylint: disable=too-many-arguments,duplicate-code
    """Persistence class."""

    def __init__(self, connection_string: str, db_tables: Dict, client: MongoClient = None):
        # for unit testing allow a mongomock client to be injected with the last parameter
        if client is None:
            client = MongoClient(connection_string)

        db_client = client[DATA_INGESTION_DATABASE_NAME]
        self.__recordings = db_client[db_tables["recordings"]]
        self.__signals = db_client[db_tables["signals"]]
        self.__pipeline_executions = db_client[db_tables["pipeline_exec"]]
        self.__algo_output = db_client[db_tables["algo_output"]]

    def update_recording_description(self, video_id, description):
        """
        Updates the description of a recording

        Args:
            video_id (str): id of the video of which the description will be updated
            description (str): new description for the video
        """
        self.__recordings.update_one(
            filter={"video_id": video_id},
            update={"$set": {"recording_overview.description": description}}
        )

    def get_signals(self, document_id: str):
        """
        Gets the signals for a given document id

        Args:
            document_id (str): id of the video from which signals will be retrieved from the database

        Raises:
            LookupError: if the recording is not found

        Returns:
            result (obj): if a document from the recordings collection has a matching id,
                          returns the document with appended matching signals from the signals collection
        """
        aggregation = [{"$match": {"video_id": document_id}}]
        aggregation.append({"$lookup": {
            "from": self.__signals.name,
            "localField": "video_id",
            "foreignField": "recording",
            "as": "signals"
        }})
        result = list(self.__recordings.aggregate(aggregation))
        if len(result) == 0:
            raise LookupError(f"Recording ID {document_id} not found")

        return result[0]

    def get_media_entry(self, recording_id: str):
        """
        Gets a single document from the recordings collection

        Args:
            recording_id (str): id of the recording to fetch

        Raises:
            LookupError: if the recording is not found

        Returns:
            result (obj): if a document from the recordings collection has a matching id, returns that document
        """
        aggregation = [{"$match": {"video_id": recording_id}}]
        result = list(self.__recordings.aggregate(aggregation))
        if len(result) == 0:
            raise LookupError(f"Recording ID {recording_id} not found")

        return result[0]

    def get_single_recording(self, recording_id: str):
        """
        Gets a single document from the recordings collection, along with matching
        information from the pipeline_exec collection

        Args:
            recording_id (str): id of the recording to fetch

        Raises:
            LookupError: if the recording is not found

        Returns:
            result (obj): if a document from the recordings collection has a matching id, returns
            that document, along with a nested matching document from the pipeline_execution database
        """
        aggregation = [{"$match": {"video_id": recording_id}}]
        aggregation.extend(self.__generate_recording_list_query())
        result = list(self.__recordings.aggregate(aggregation))

        if len(result) == 0:
            raise LookupError(f"Recording ID {recording_id} not found")

        return result[0]

    def get_recording_list(self, page_size, page, additional_query, order, aggregation_pipeline_prefix=None):
        """Get all videos that entered processing phase or the specific one video."""
        if aggregation_pipeline_prefix is None:
            aggregation_pipeline_prefix = []

        aggregation_pipeline = self.__generate_recording_list_query(
            additional_query, order, aggregation_pipeline_prefix)

        skip_entries = (page - 1) * page_size
        count_facet = [{"$count": "number_recordings"}]
        result_facet = [
            {"$skip": skip_entries},
            {"$limit": page_size}
        ]
        aggregation_pipeline.append(
            {"$facet": {"count": count_facet, "result": result_facet}})

        pipeline_result = self.__recordings.aggregate(
            aggregation_pipeline).next()

        count_result = pipeline_result["count"]
        number_recordings = int(
            count_result[0]["number_recordings"]) if count_result else 0
        number_pages = ceil(float(number_recordings) / page_size)

        return pipeline_result["result"], number_recordings, number_pages

    def __generate_recording_list_query(self, additional_query=None, sorting=None, aggregation_pipeline_prefix=None):
        """
        Generates aggregation steps to facilitate matching documents from the recordings collection
        with documents from the pipeline_execution collection

        Args:
            additional_query (obj): additional query to match documents on pipeline_execution collection
            sorting (obj): additional query to sort matched documents
            aggregation_pipeline_prefix (obj): additional query prefix for the aggregation

        Returns:
            aggregation (list): list of aggregation steps to form a query
        """
        if aggregation_pipeline_prefix is None:
            aggregation_pipeline_prefix = []

        aggregation = copy.deepcopy(aggregation_pipeline_prefix)
        aggregation.append({"$match": {"_media_type": "video"}})
        aggregation.append({"$lookup": {
            "from": self.__pipeline_executions.name,
            "localField": "video_id",
            "foreignField": "_id",
            "as": "pipeline_execution"
        }})
        aggregation.append({"$unwind": "$pipeline_execution"})

        if additional_query:
            aggregation.append({"$match": additional_query})
        if sorting:
            aggregation.append({"$sort": sorting})
        else:
            aggregation.append({"$sort": {"recording_overview.time": -1}})
        return aggregation

    def get_algo_output(self, algo, recording_id):
        """
        Gets a single document from the algo_output collection

        Args:
            algo (str): id of the algorithm to look for
            recording_id (str): id of the recording to look for

        Returns:
            entry (obj): if a document from the algo_output collection has a matching id, returns that document
        """
        entry = self.__algo_output.find_one(
            {"algorithm_id": algo, "pipeline_id": recording_id})
        return entry
