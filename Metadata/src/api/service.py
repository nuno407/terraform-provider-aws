from ast import operator
from datetime import timedelta
import json
import logging
import re

from api.db import Persistence


class ApiService:

    def __init__(self, db: Persistence, s3):
        self.__db = db
        self.__s3 = s3

    def get_video_signals(self, video_id):
        recording_item = self.__db.get_signals(video_id)
        signals = {}
        for signal_group in recording_item['signals']:
            if (signal_group['source'] == "MDF"):
                signals[signal_group['source']] = self.__create_video_signals_object(signal_group)
            else:    
                signals[signal_group['algo_out_id'].split('_')[-1]] = self.__create_video_signals_object(signal_group)
        return signals

    def __create_video_signals_object(self, chc_result):
        result_signals = {}
        relevant_signals = ["interior_camera_health_response_cvb", "interior_camera_health_response_cve", "CameraViewBlocked", "CameraViewShifted", "interior_camera_health_response_audio_blocked", "interior_camera_health_response_audio_distorted", "interior_camera_health_response_audio_signal"]
        if chc_result['signals'] and len(chc_result['signals']) > 0 and type(list(chc_result['signals'].values())[0]) is dict:
            for timestamp, signals in chc_result['signals'].items():
                result_signals[timestamp] = {key: signals[key] for key in relevant_signals if key in signals}

        elif chc_result['signals'] and type(chc_result['signals']):
            for k, v in chc_result['signals'].items():
                result_signals[k] = {}
                result_signals[k]['CameraViewBlocked'] = v

        return result_signals

    def update_video_description(self, id, description):
        self.__db.update_recording_description(id, description)

    def create_anonymized_video_url(self, recording_id):
        url = None
        entry = self.__db.get_algo_output("Anonymize", recording_id)
        if entry:
            # Get video path and split it into bucket and key
            s3_path = entry['output_paths']['video']
            bucket, path = s3_path.split("/", 1)
            url = self.__create_video_url(bucket, path)
        return url

    def create_video_url(self, bucket, folder, file):
        path = folder + file
        return self.__create_video_url(bucket, path)

    def __create_video_url(self, bucket, path):
        params_s3 = {'Bucket': bucket, 'Key': path}
        url = self.__s3.generate_presigned_url('get_object',
                                                            Params = params_s3)
        return url

    def get_table_data(self, page_size, page, query, operator, sorting, direction):
        additional_query = None
        if query and operator:
            additional_query = self.__parse_query(query, operator)
        sorting_query = None
        if sorting and direction:
            sorting_query = self.__parse_sorting(sorting, direction)

        recordings, number_recordings, number_pages = self.__db.get_recording_list(page_size, page, additional_query, sorting_query)
        table_data = [self.__map_recording_object(r) for r in recordings]
        return table_data, number_recordings, number_pages
        
    # State all valid query fields and their corresponding database field path
    __query_fields = {
                    "_id": "video_id",
                    "processing_list": "pipeline_execution.processing_list",
                    'snapshots': 'recording_overview.#snapshots',
                    'data_status': 'pipeline_execution.data_status',
                    'last_updated': 'pipeline_execution.last_updated',
                    'length': 'recording_overview.length',
                    'time': 'recording_overview.time',
                    'resolution': 'recording_overview.resolution',
                    'deviceID': 'recording_overview.deviceID',
                    'tenantID': 'recording_overview.tenantID'
                    }

    def __parse_query(self, query, logic_operator):
        # State all valid logical operators and their corresponding mongo query
        logic_operators = {
                        "or":"$or",
                        "and":"$and"
                        }

        # State all valid query operators and their corresponding ongo operators 
        # (used for validation and later conversion)
        operators = {
                        "==":"$eq",
                        ">":"$gt",
                        "<":"$lt",
                        "!=":"$ne",
                        "has":"$regex"
                        }

        # Check if operator is valid
        assert logic_operator.lower() in logic_operators.keys(), "Invalid/Forbidden logical operator"

        # Check if all query fields are valid
        assert [fieldname for fieldname in query.keys() if fieldname not in self.__query_fields.keys()] == [], "Invalid/Forbidden query keys"

        # Create empty list that will contain all sub queries received
        query_list = []
        
        for fieldname, field_query in query.items():
            operator = list(field_query.keys())[0]
            operation_value = field_query[operator]

            ## OPERATOR VALIDATION + CONVERSION
            # Check if operator is valid 
            assert operator in operators.keys(), "Invalid/Forbidden query operators"

            # Convert the operator to mongo syntax
            mongo_operator = operators[operator]

            ## VALUE VALIDATION
            # Check if value is valid (i.e. alphanumeric and/or with characters _ : . -)
            # NOTE: spaces are allowed in the value string
            assert re.findall("^[a-zA-Z0-9_:.\s-]*$", str(operation_value)) != [], "Invalid/Forbidden query values"

            # Create subquery. 
            # NOTE: $exists -> used to make sure items without
            # the parameter set in key are not also returned
            subquery = {self.__query_fields[fieldname]:{'$exists': 'true', mongo_operator:operation_value}}

            # Append subquery to list
            query_list.append(subquery)

        # Append selected logical operator to query
        # NOTE: Resulting format -> { <AND/OR>: [<subquery1>, <subquery2>, ...] },
        #       which translates into: <subquery1> AND/OR <subquery2> AND/OR ... 
        return {logic_operators[logic_operator.lower()]: query_list}

        
    def __parse_sorting(self, sorting, direction):
        directions = {
            "asc": 1,
            "dsc": -1
        }

        # Check if operator is valid
        assert direction.lower() in directions.keys(), "Invalid/Forbidden sorting direction"

        # Check if the sorting field is valid
        assert sorting in self.__query_fields.keys(), "Invalid/Forbidden sorting field"

        return {self.__query_fields[sorting]: directions[direction]}



    def get_single_recording(self, recording_id):
        recording_item = self.__db.get_single_recording(recording_id)
        result = self.__map_recording_object(recording_item)

        # add LQ video info if neccessary
        lq_video = self.__check_and_get_lq_video_info(recording_id)
        if lq_video:
            result['lq_video'] = lq_video
        return result

    def __map_recording_object(self, recording_item):
        recording_object = {}
        # Add CHC information
        recording_object['number_CHC_events'] = ''      
        recording_object['lengthCHC'] = ''
        
        recording_overview = recording_item.get('recording_overview',{})
        recording_object['tenant'] = recording_overview.get('tenantID','-')
        recording_object['snapshots'] = recording_overview.get('#snapshots','-')
        recording_object['length'] = recording_overview.get('length','-')
        recording_object['time'] = recording_overview.get('time','-')   
        recording_object['deviceID'] = recording_overview.get('deviceID','-')
        recording_object['description'] = recording_overview.get('description')

        recording_object['_id'] = recording_item.get('video_id')
        recording_object['resolution'] = recording_item.get('resolution','-')      
        

        pipeline_execution = recording_item.get('pipeline_execution',{})
        recording_object['processing_list'] = pipeline_execution.get('processing_list','-')      
        recording_object['data_status'] = pipeline_execution.get('data_status','-')                      
        recording_object['last_updated'] = pipeline_execution.get('last_updated','2020-01-01T00:00:00.1Z').split(".",1)[0].replace("T"," ")
                    
        
        return recording_object

        

    def __calculate_chc_events(self, chc_periods):
        duration = 0.0
        number = 0
        for period in chc_periods:
            duration += period['duration']
            if period['duration'] > 0.0:
                number += 1

        return number, duration

    def __check_and_get_lq_video_info(self, entry_id):
        recorder_name_matcher = re.match(r".+_([^_]+)_\d+_\d+", entry_id)
        if not recorder_name_matcher or len(recorder_name_matcher.groups()) != 1:
            logging.warning(f'Could not parse recorder information from {entry_id}')
            return None
        
        if recorder_name_matcher.group(1) != 'TrainingRecorder':
            logging.debug(f'Skipping LQ video search for {entry_id} because it is recorded with {recorder_name_matcher.group(1)}')
            return None
        lq_id = entry_id.replace('TrainingRecorder', 'InteriorRecorder')
        try:
            lq_entry = self.__db.get_single_recording(lq_id)
        except:
            return None
        lq_video_details = lq_entry.get('recording_overview')

        lq_video = {}
        lq_video['id'] = lq_id
        lq_video['length'] = lq_video_details['length']
        lq_video['time'] = lq_video_details['time']                
        lq_video['resolution'] = lq_entry['resolution']        
        lq_video['snapshots'] = lq_video_details['#snapshots']

        return lq_video