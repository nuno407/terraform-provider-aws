from datetime import timedelta
import logging
from baseaws.timestamps import from_epoch_seconds_or_milliseconds
from typing import Any, Union

_logger = logging.getLogger('mdfparser.' + __name__)

class Synchronizer:
    def __init__(self) -> None:
        pass

    def synchronize(self, mdf: dict[str, Any], recording_epoch_from: int , recording_epoch_to:int )->dict[timedelta, dict[str, Union[bool,float,int]]]:
        _logger.debug('Synchronizing MDF from %s to %s', recording_epoch_from, recording_epoch_to)

        # check MDF format
        if not ('chunk' in mdf and all(k in mdf['chunk'] for k in ('pts_start', 'pts_end', 'utc_start', 'utc_end'))
                and 'frame' in mdf):
            raise InvalidMdfException('Not all required keys exist in the passed MDF.')

        pts_start= int(mdf['chunk']['pts_start'])
        pts_end= int(mdf['chunk']['pts_end'])
        epoch_start= int(mdf['chunk']['utc_start'])
        epoch_end= int(mdf['chunk']['utc_end'])
        recording_start = from_epoch_seconds_or_milliseconds(recording_epoch_from)
        recording_end = from_epoch_seconds_or_milliseconds(recording_epoch_to)

        # epoch calculation formula
        pts_to_epoch_factor = (epoch_end - epoch_start) / (pts_end - pts_start)

        # frames processing

        sync_frames = {}
        for frame in mdf["frame"]:

            # timestamp calculation and processing
            frame_timestamp_epoch = (int(frame.get('timestamp')) - pts_start) * pts_to_epoch_factor + epoch_start
            frame_timestamp_absolute = from_epoch_seconds_or_milliseconds(frame_timestamp_epoch)
            
            if frame_timestamp_absolute < recording_start or frame_timestamp_absolute > recording_end:
                # skip frames of MDF that are outside the recording time
                continue
            
            # note that this timestamp will be relative to the beginning of the recording
            frame_timestamp = frame_timestamp_absolute - recording_start

            signals = self.__get_frame_signals(frame)            

            if len(signals) > 0:
                sync_frames[frame_timestamp] = signals

        _logger.debug('Finished synchronizing MDF from %s to %s', recording_epoch_from, recording_epoch_to)
        return sync_frames
        
    def __get_frame_signals(self, frame: dict)->dict[str, Union[bool,float,int]]:
        signals: dict[str, Union[bool,float,int]] = {}
        if 'objectlist' in frame.keys():
            for item in frame['objectlist']:
                signals.update(self.__extract_bools(item))
                signals.update(self.__extract_floats(item))
                signals.update(self.__extract_ints(item))
        return signals

    def __extract_bools(self, frame_objects: dict)->dict[str, bool]:
        values: dict[str, bool] = {}
        if 'boolAttributes' in frame_objects:
            for attribute in frame_objects['boolAttributes']:
                if 'name' in attribute and 'value' in attribute:
                    values[attribute['name']] = (
                        attribute['value'] == 'true')
        return values

    def __extract_floats(self, frame_objects: dict)->dict[str, float]:
        values: dict[str, float] = {}
        if 'floatAttributes' in frame_objects:
            for attribute in frame_objects['floatAttributes']:
                if 'name' in attribute and 'value' in attribute:
                    values[attribute['name']] = float(attribute['value'])
        return values

    def __extract_ints(self, frame_objects: dict)->dict[str, int]:
        values: dict[str, int] = {}
        if 'integerAttributes' in frame_objects:
            for attribute in frame_objects['integerAttributes']:
                if 'name' in attribute and 'value' in attribute:
                    values[attribute['name']] = int(attribute['value'])
        return values

class InvalidMdfException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
