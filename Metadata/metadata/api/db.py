# type: ignore
"""Metadata api database controller."""
import copy
from math import ceil
from typing import Dict

from pymongo.mongo_client import MongoClient

from base.aws.container_services import DATA_INGESTION_DATABASE_NAME


class Persistence:
    """Persistence class."""

    def __init__(self, connection_string: str, db_tables: Dict, client: MongoClient = None):
        # for unit testing allow a mongomock client to be injected with the last parameter
        if client is None:
            client = MongoClient(connection_string)

        db_client = client[DATA_INGESTION_DATABASE_NAME]
        self.__recordings = db_client[db_tables['recordings']]
        self.__signals = db_client[db_tables['signals']]
        self.__pipeline_executions = db_client[db_tables['pipeline_exec']]
        self.__algo_output = db_client[db_tables['algo_output']]

    def update_recording_description(self, video_id, description):
        self.__recordings.update_one(
            filter={"video_id": video_id},
            update={"$set": {'recording_overview.description': description}}
        )

    def get_signals(self, document_id: str):
        aggregation = [{"$match": {'video_id': document_id}}]
        aggregation.append({'$lookup': {
            'from': self.__signals.name,
            'localField': 'video_id',
            'foreignField': 'recording',
            'as': 'signals'
        }})
        result = list(self.__recordings.aggregate(aggregation))
        if len(result) == 0:
            raise LookupError(f'Recording ID {document_id} not found')

        return result[0]

    def get_media_entry(self, recording_id: str):
        aggregation = [{"$match": {'video_id': recording_id}}]
        result = list(self.__recordings.aggregate(aggregation))
        if len(result) == 0:
            raise LookupError(f'Recording ID {recording_id} not found')

        return result[0]

    def get_single_recording(self, recording_id: str):
        aggregation = [{"$match": {'video_id': recording_id}}]
        aggregation.extend(self.__generate_recording_list_query())
        result = list(self.__recordings.aggregate(aggregation))

        if len(result) == 0:
            raise LookupError(f'Recording ID {recording_id} not found')

        return result[0]

    def get_recording_list(self, page_size, page, additional_query, order, aggregation_pipeline_prefix=[]):
        """Get all videos that entered processing phase or the specific one video."""
        aggregation_pipeline = self.__generate_recording_list_query(
            additional_query, order, aggregation_pipeline_prefix)

        skip_entries = (page - 1) * page_size
        count_facet = [{'$count': 'number_recordings'}]
        result_facet = [
            {'$skip': skip_entries},
            {'$limit': page_size}
        ]
        aggregation_pipeline.append(
            {'$facet': {'count': count_facet, 'result': result_facet}})

        pipeline_result = self.__recordings.aggregate(
            aggregation_pipeline).next()

        count_result = pipeline_result['count']
        number_recordings = int(
            count_result[0]['number_recordings']) if count_result else 0
        number_pages = ceil(float(number_recordings) / page_size)

        return pipeline_result['result'], number_recordings, number_pages

    def __generate_recording_list_query(self, additional_query=None, sorting=None, aggregation_pipeline_prefix=[]):
        aggregation = copy.deepcopy(aggregation_pipeline_prefix)
        aggregation.append({"$match": {'_media_type': "video"}})
        aggregation.append({'$lookup': {
            'from': self.__pipeline_executions.name,
            'localField': 'video_id',
            'foreignField': '_id',
            'as': 'pipeline_execution'
        }})
        aggregation.append({'$unwind': '$pipeline_execution'})

        if (additional_query):
            aggregation.append({"$match": additional_query})
        if (sorting):
            aggregation.append({'$sort': sorting})
        else:
            aggregation.append({'$sort': {'recording_overview.time': -1}})
        return aggregation

    def get_algo_output(self, algo, recording_id):
        entry = self.__algo_output.find_one(
            {"algorithm_id": algo, "pipeline_id": recording_id})
        return entry
