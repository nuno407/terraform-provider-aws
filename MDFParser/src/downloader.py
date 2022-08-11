import json
import logging
from s3_interaction import S3Interaction

from baseaws.shared_functions import ContainerServices

_logger = logging.getLogger('mdfparser.' + __name__)
class Downloader(S3Interaction):
    def download(self, mdf_path: str)->dict:
        _logger.debug('Downloading metadata from %s', mdf_path)
        bucket, key = self._get_s3_path(mdf_path)
        binary_data = ContainerServices.download_file(self._s3_client, bucket, key)
        json_string = binary_data.decode('utf-8')
        mdf = json.loads(json_string)

        # check if epoch timestamps are in the mdf or recreates them if possible
        if (not ('chunk' in mdf and 'utc_start' in mdf['chunk'] and 'utc_end' in mdf['chunk']) and
            'chunk' in mdf and 'pts_start' in mdf['chunk'] and 'pts_end' in mdf['chunk']):
            try:
                utc_start, utc_end = self.__recreate_epoch_timestamps(bucket, key, mdf['chunk']['pts_start'], mdf['chunk']['pts_end'])
                mdf['chunk']['utc_start'] = utc_start
                mdf['chunk']['utc_end'] = utc_end
            except Exception:
                _logger.exception('Error recreating epoch timestamps from compact MDF.')
        _logger.debug('Finished metadata download from %s', mdf_path)
        return mdf
    
    def __recreate_epoch_timestamps(self, bucket: str, mdf_key: str, mdf_pts_start: int, mdf_pts_end: int)->tuple[int, int]:
        key = mdf_key.replace('_metadata_full', '_compact_mdf')
        binary_data = ContainerServices.download_file(self._s3_client, bucket, key)
        json_string = binary_data.decode('utf-8')
        compact_mdf = json.loads(json_string)
        if not('partial_timestamps' in compact_mdf and len(compact_mdf['partial_timestamps']) > 1):
            raise InvalidCompactMdfException('partial_timestamps object not in compact MDF or only consists of one entry')
        pts_infos: list = list(compact_mdf['partial_timestamps'].values())
        pts_start: int = pts_infos[0]['pts_start']
        epoch_start: int = pts_infos[0]['converted_time']
        pts_end: int = pts_infos[-1]['pts_start']
        epoch_end: int = pts_infos[-1]['converted_time']

        # epoch calculation formula
        pts_to_epoch_factor = (epoch_end - epoch_start) / (pts_end - pts_start)
        epoch_start_mdf = (mdf_pts_start - pts_start) * pts_to_epoch_factor + epoch_start
        epoch_end_mdf = (mdf_pts_end - pts_end) * pts_to_epoch_factor + epoch_end
        return round(epoch_start_mdf), round(epoch_end_mdf)
        
class InvalidCompactMdfException(Exception):
    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
