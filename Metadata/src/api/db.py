from math import ceil
import pymongo

DB_NAME = "DataIngestion"

class Persistence:

    def __init__(self, connection_string, db_tables, client = None):
        # for unit testing allow a mongomock client to be injected with the last parameter
        if client == None:
            client = pymongo.MongoClient(connection_string)
        db_client = client[DB_NAME]
        self.__recordings = db_client[db_tables['recordings']]
        self.__signals = db_client[db_tables['signals']]
        self.__pipeline_executions = db_client[db_tables['pipeline_exec']]
        self.__algo_output = db_client[db_tables['algo_output']]

    def update_recording_description(self, id, description):
        self.__recordings.update_one(
            { "video_id" : id },
            { "$set": { 'recording_overview.description' : description } }
        )

    def get_signals(self, id):
        aggregation = [{'$match': {'video_id': id}}]
        aggregation.append({'$lookup': {'from':self.__signals.name, 'localField':'video_id', 'foreignField':'recording', 'as': 'signals'}})
        result = list(self.__recordings.aggregate(aggregation))
        if(len(result) == 0): raise LookupError('Recording ID ' + id + ' not found')
        else: return result[0]

    def get_single_recording(self, recording_id):
        aggregation = [{'$match': {'video_id': recording_id}}]
        aggregation.extend(self.__generate_recording_list_query())
        result = list(self.__recordings.aggregate(aggregation))
        if(len(result) == 0): raise LookupError('Recording ID ' + recording_id + ' not found')
        else: return result[0]

    def get_recording_list(self, page_size, page, additional_query, order):
        # Get all videos that entered processing phase or the specific one video
        aggregation_pipeline = self.__generate_recording_list_query(additional_query, order)

        skip_entries = (page - 1) * page_size
        count_facet = [{'$count': 'number_recordings'}]
        result_facet = [
            {'$skip': skip_entries},
            {'$limit': page_size}
        ]
        aggregation_pipeline.append({'$facet': {'count': count_facet, 'result': result_facet}})
        
        pipeline_result = self.__recordings.aggregate(aggregation_pipeline).next()

        count_result = pipeline_result['count']
        number_recordings = int(count_result[0]['number_recordings']) if count_result else 0
        number_pages = ceil(float(number_recordings) / page_size)

        return pipeline_result['result'], number_recordings, number_pages

    def __generate_recording_list_query(self, additional_query = None, sorting = None):
        aggregation = []
        aggregation.append({'$match': {'_media_type':"video"}})
        aggregation.append({'$lookup': {'from':self.__pipeline_executions.name, 'localField':'video_id', 'foreignField':'_id', 'as': 'pipeline_execution'}})
        aggregation.append({'$unwind': '$pipeline_execution'})
        aggregation.append({'$match':{'pipeline_execution.data_status':'complete'}})

        if(additional_query):
            aggregation.append({'$match': additional_query})
        if(sorting):
            aggregation.append({'$sort': sorting})
        else:
            aggregation.append({'$sort': {'recording_overview.time':1}})
        return aggregation

    def get_algo_output(self, algo, recording_id):
        entry = self.__algo_output.find_one({"algorithm_id":algo, "pipeline_id": recording_id})
        return entry

    