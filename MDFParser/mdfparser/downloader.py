""" S3 Downloader module. """
import json
import logging
from mdfparser.s3_interaction import S3Interaction

from base.aws.container_services import ContainerServices

_logger = logging.getLogger("mdfparser." + __name__)


class InvalidCompactMdfException(Exception):
    """Exception raised when compact MDF is invalid."""
    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)


class Downloader(S3Interaction): # pylint: disable=too-few-public-methods
    """ S3 Downloader class. """

    def download(self, mdf_path: str) -> dict:
        """Downloads metadata full file from S3.

        Args:
            mdf_path (str): path of the metadata full file in S3.

        Returns:
            dict: metadata full file as dictionary.
        """
        _logger.debug("Downloading metadata from %s", mdf_path)
        bucket, key = self._get_s3_path(mdf_path)
        binary_data = ContainerServices.download_file(self._s3_client, bucket, key)
        json_string = binary_data.decode("utf-8")
        mdf = json.loads(json_string)

        # check if epoch timestamps are in the mdf or recreates them if possible
        if (not ("chunk" in mdf and "utc_start" in mdf["chunk"] and "utc_end" in mdf["chunk"]) and
                "chunk" in mdf and "pts_start" in mdf["chunk"] and "pts_end" in mdf["chunk"]):
            try:
                utc_start, utc_end = self.__recreate_epoch_timestamps(
                    bucket, key, mdf["chunk"]["pts_start"], mdf["chunk"]["pts_end"])
                mdf["chunk"]["utc_start"] = utc_start
                mdf["chunk"]["utc_end"] = utc_end
            except InvalidCompactMdfException as err:
                _logger.exception("Error recreating epoch timestamps from compact MDF: %s", err)
            except Exception as err: # pylint: disable=broad-except
                _logger.exception("Unexpected error: %s", err)
        else:
            # check for pts_end swapped with utc_start:
            # unfortunately some many MDF files have been generated with this bug
            # (https://github.com/bosch-rc-dev/container_scripts/commit/aba854b68862eb186db17507ceeb50d4d56445c0) # pylint: disable=line-too-long
            pts_end = mdf["chunk"]["pts_end"]
            utc_start = mdf["chunk"]["utc_start"]
            if (len(str(pts_end)) == 13 and str(pts_end).startswith("16")):
                mdf["chunk"]["pts_end"] = utc_start
                mdf["chunk"]["utc_start"] = pts_end
                _logger.warning(
                    "Auto-correcting swapped pts_end and utc_start in MDF file %s", mdf_path)
        _logger.debug("Finished metadata download from %s", mdf_path)
        return mdf

    def __recreate_epoch_timestamps(self, bucket: str, mdf_key: str, mdf_pts_start: int,
                                    mdf_pts_end: int) -> tuple[int, int]:
        """Recreates epoch timestamps from compact MDF. """
        key = mdf_key.replace("_metadata_full", "_compact_mdf")
        binary_data = ContainerServices.download_file(self._s3_client, bucket, key)
        compact_mdf = json.loads(binary_data.decode("utf-8"))
        if not ("partial_timestamps" in compact_mdf and len(compact_mdf["partial_timestamps"]) > 1):
            raise InvalidCompactMdfException(
                "partial_timestamps object not in compact MDF or only consists of one entry")

        pts_infos: list = list(compact_mdf["partial_timestamps"].values())
        pts_start: int = pts_infos[0]["pts_start"]
        epoch_start: int = pts_infos[0]["converted_time"]
        pts_end: int = pts_infos[-1]["pts_start"]
        epoch_end: int = pts_infos[-1]["converted_time"]

        # epoch calculation formula
        pts_to_epoch_factor = (epoch_end - epoch_start) / (pts_end - pts_start)
        return self.__calculate_interval(mdf_pts_start, mdf_pts_end, pts_to_epoch_factor,
                                         epoch_start, epoch_end)

    def __calculate_interval(self, pts_start: int, pts_end: int, pts_to_epoch_factor: float, # pylint: disable=too-many-arguments
                             epoch_start: int, epoch_end: int) -> tuple[int, int]:
        """Calculates epoch timestamps from MDF. """
        return (round((pts_start - pts_end) * pts_to_epoch_factor + epoch_end),
                round((pts_end - pts_start) * pts_to_epoch_factor + epoch_start))
