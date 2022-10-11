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



    def get_video_snapshot_media(self, deviceID, tenantID, start, end, media_type):

        result = []
        #If end is 0 it's a snapshot, get all videos
        if media_type == "image" :
            aggregation = [{'$match': {'recording_overview.deviceID': deviceID, 'recording_overview.tenantID': tenantID, '_media_type' : "video"}}]
        #Else it's a video, get all snapshots
        elif media_type == "video":
            #all snapshots for that tennant and device
            aggregation = [{'$match': {'recording_overview.deviceID': deviceID, 'recording_overview.tenantID': tenantID, '_media_type' : "image"}}]

        related = list(self.__recordings.aggregate(aggregation))

        if media_type == "image" :
            for related_item in related:
                name_split = related_item["video_id"].split("_")
                related_start_time = name_split[-2]
                related_end_time = name_split[-1]
                #print('start: '+str(start)+' related_start_time: '+str(related_start_time)+' related_end_time: '+str(related_end_time))
                if (int(start) >= int(related_start_time) and int(start) <= int(related_end_time) ):
                    result.append(related_item['video_id'])
        elif media_type == "video":
            for related_item in related:
                name_split = related_item["video_id"].split("_")
                related_time = name_split[-1]
                #print('related_time: '+related_time+' start: '+start+' end: '+end)
                if (int(related_time) >= int(start) and int(related_time) <= int(end) ):
                    result.append(related_item['video_id'])
                    #Improve, replace all processing to be in the query
                    #aggregation.append({'$match': {'deviceID': deviceID, 'tenantID': tenantID, 'video_id': {'$regex' : "Snapshot"}, { '$convert': { 'input': { '$split': [ 'video_id', "-" ][-2] }, to: "int" } } :  { '$gt': video_start }, { '$convert': { 'input': { '$split': [ 'video_id', "-" ][-2] }, to: "int" } } :  { '$lt': video_end }}})
        return result

    def get_media_entry(self, recording_id):
        recording_item = self.__db.get_media_entry(recording_id)
        result = self.__map_recording_object(recording_item)
        return result
