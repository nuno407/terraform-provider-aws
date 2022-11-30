"""CHC Synchronizer module."""
import json
import logging
from datetime import timedelta
from math import isclose
from typing import Dict
from typing import Type
from typing import TypeVar
from typing import Union

import boto3

EXPECTED_FPS = 15.72

_logger = logging.getLogger(__name__)


class ChcSynchronizer:
    """CHC Synchronizer class."""

    def __init__(self) -> None:
        self.__s3_client = boto3.client('s3', region_name='eu-central-1')

    def download(self, bucket: str, key: str) -> Dict:
        """Downloads CHC results from S3 bucket.

        Args:
            bucket (str): S3 bucket name
            key (str): S3 object key
        Returns:
            dict: parsed CHC data
        """
        _logger.debug('Downloading CHC results from %s/%s', bucket, key)
        binary_data = self.__s3_client.get_object(
            Bucket=bucket, Key=key)['Body'].read()
        json_string = binary_data.decode('utf-8')
        data = json.loads(json_string)
        _logger.debug(
            'Finished downloading CHC results from %s/%s', bucket, key)
        return data

    def synchronize(self, chc_output: dict,
                    video_length: timedelta) -> Dict[timedelta, Dict[str, Union[bool, float, int]]]:
        """Synchronize CHC metadata with video timestamps.

        Args:
            chc_output (dict): CHC parsed data
            video_length (timedelta): timedelta object that represents the video duration

        Returns:
            dictionary correlating timestamps with CHC signal data
        """
        _logger.debug('Starting synchronization of CHC results')
        frames = chc_output['frame']
        actual_fps = len(frames) / video_length.total_seconds()
        _logger.debug('Calculated a framerate of %f FPS', actual_fps)
        if not isclose(actual_fps, EXPECTED_FPS, abs_tol=0.1):
            _logger.warning(
                'FPS mismatch when synchronizing CHC data: Actual FPS is %f instead of the expected %f',
                actual_fps,
                EXPECTED_FPS)

        sync_frames = {}
        frame_offset = video_length / len(frames)
        for frame_number, frame in enumerate(frames):
            sync_frames[frame_number *
                        frame_offset] = self.__get_frame_signals(frame)
        _logger.debug('Finished synchronization of CHC results')
        return sync_frames

    def __get_frame_signals(self, frame: Dict) -> Dict[str, Union[bool, float, int]]:
        """Gets fill signals for a given frame and returns a dictionary with all signals in a given frame.

        Args:
            frame (dict): video frame dictionary
        Returns:
            signals dictionary for a given frame
        """
        signals: Dict[str, Union[bool, float, int]] = {}
        if 'objectlist' in frame.keys():
            for item in frame['objectlist']:
                signals.update(self.__extract_vars(
                    item, bool, 'boolAttributes'))
                signals.update(self.__extract_vars(
                    item, float, 'floatAttributes'))
                signals.update(self.__extract_vars(
                    item, int, 'integerAttributes'))
        return signals

    T = TypeVar('T', bool, float, int)

    def __extract_vars(self, frame_objects: dict, var_type: Type[T], sub_struct_name: str) -> Dict[str, T]:
        """Extract vars and perform necessary casting operations.

        Treats input frame variables making the necessary cast operations to a given variable type,
        boolean type is represented as string in the output dictionary

        Args:
            frame_objects (dict): a dictionary of frame objects
            var_type (Type[T]): current variable type for casting purposes
            sub_struct_name (str): key of the substructure to be converted
        Returns:
            dictionary correlating attribute keys and values properly converted to the input type
        """
        values = {}
        if sub_struct_name in frame_objects:
            for attribute in frame_objects[sub_struct_name]:
                if 'name' in attribute and 'value' in attribute:
                    if (var_type == bool):
                        values[attribute['name']] = attribute['value'] == 'true'
                    else:
                        values[attribute['name']] = var_type(
                            attribute['value'])
        return values
