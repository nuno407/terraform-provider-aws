"""S3 downloader"""
import json
from kink import inject
from base.aws.s3 import S3Controller


@inject
class S3Downloader:  # pylint: disable=too-few-public-methods
    """
    Downloads data from DevCloud
    """

    def __init__(self, s3_controller: S3Controller):
        """
        Constructor

        Args:
            s3_controller (S3Controller): The s3 controller
        """
        self.__s3_controller = s3_controller

    def download_convert_json(self, s3_path: str) -> dict:
        """
        Downloads json files

        Args:
            s3_path (str): s3_path to file

        Returns:
            dict: The json data
        """
        bucket, path = self.__s3_controller.get_s3_path_parts(s3_path)
        raw_data = self.__s3_controller.download_file(bucket, path)
        str_data = raw_data.decode()
        return json.loads(str_data)
