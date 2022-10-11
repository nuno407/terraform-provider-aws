from datetime import timedelta
import json
import logging
from math import isclose
from typing import Any, Type, TypeVar, Union
import boto3

EXPECTED_FPS = 15.72

_logger = logging.getLogger(__name__)

class ChcSynchronizer:
    def __init__(self) -> None:
        self.__s3_client = boto3.client('s3', region_name='eu-central-1')

    def download(self, bucket: str, key: str)->dict:
        _logger.debug('Downloading CHC results from %s/%s', bucket, key)
        binary_data = self.__s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
        json_string = binary_data.decode('utf-8')
        data = json.loads(json_string)
        _logger.debug('Finished downloading CHC results from %s/%s', bucket, key)
        return data

    def synchronize(self, chc_output: dict, video_length: timedelta) -> dict[timedelta, dict[str, Union[bool,float,int]]]:
        _logger.debug('Starting synchronization of CHC results')
        frames = chc_output['frame']
        actual_fps = len(frames) / video_length.total_seconds()
        _logger.debug('Calculated a framerate of %f FPS', actual_fps)
        if not isclose(actual_fps, EXPECTED_FPS, abs_tol=0.1):
            _logger.warning(f'FPS mismatch when synchronizing CHC data: Actual FPS is {actual_fps} instead of the expected {EXPECTED_FPS}')

        sync_frames = {}
        frame_offset = video_length/len(frames)
        for frame_number, frame in enumerate(frames):
            sync_frames[frame_number * frame_offset] = self.__get_frame_signals(frame)
        _logger.debug('Finished synchronization of CHC results')
        return sync_frames


    def __get_frame_signals(self, frame: dict)->dict[str, Union[bool,float,int]]:
        signals: dict[str, Union[bool,float,int]] = {}
        if 'objectlist' in frame.keys():
            for item in frame['objectlist']:
                signals.update(self.__extract_vars(item, bool, 'boolAttributes'))
                signals.update(self.__extract_vars(item, float, 'floatAttributes'))
                signals.update(self.__extract_vars(item, int, 'integerAttributes'))
        return signals

    T = TypeVar('T', bool, float, int)
    def __extract_vars(self, frame_objects: dict, var_type: Type[T], sub_struct_name: str)->dict[str, T]:
        values = {}
        if sub_struct_name in frame_objects:
            for attribute in frame_objects[sub_struct_name]:
                if 'name' in attribute and 'value' in attribute:
                    if(var_type == bool):
                        values[attribute['name']] = attribute['value'] == 'true'
                    else:
                        values[attribute['name']] = var_type(attribute['value'])
        return values
