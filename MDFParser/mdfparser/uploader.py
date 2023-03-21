""" This module contains the Uploader class. """
from datetime import timedelta
import json
import logging
from typing import Union
from mdfparser.s3_interaction import S3Interaction

from base.aws.container_services import ContainerServices

_logger = logging.getLogger("mdfparser." + __name__)

class Uploader(S3Interaction): # pylint: disable=too-few-public-methods
    """ This class contains methods to upload files to S3. """
    def upload_signals(self, signals: dict[timedelta, dict[str, Union[bool, int, float]]], s3_path_mdf: str)->dict[str, str]: # pylint: disable=line-too-long
        """ Uploads synchronized signals to S3. """
        bucket, key = self._get_s3_path(s3_path_mdf)
        key = key.replace("_metadata_full.json", "_signals.json")
        _logger.info("Uploading synchronized signals to S3 path [%s]", s3_path_mdf)

        payload = {str(time): value for time, value in signals.items()}
        bin_payload: bytes = json.dumps(payload).encode("UTF-8")
        ContainerServices.upload_file(self._s3_client, bin_payload, bucket, key)

        _logger.debug("Finished uploading synchronized signals")
        return {"bucket": bucket, "key": key}
