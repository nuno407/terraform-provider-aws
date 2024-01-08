""" Request Factory """
from enum import Enum
from kink import inject
from requests import Request
from base.model.base_model import ConfiguredBaseModel
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.config import ArtifactDownloaderConfig


class PartialEndpoint(str, Enum):
    """ Contains all the endpoints to be used """
    RC_SIGNALS_VIDEO = "ridecare/signals/video"
    RC_SIGNALS_SNAPSHOT = "ridecare/signals/snapshot"
    RC_VIDEO = "ridecare/video"
    RC_VIDEO_UPLOAD_RULE = "ridecare/upload_rule/video"
    RC_SNAPSHOT = "ridecare/snapshots"
    RC_SNAPSHOT_UPLOAD_RULE = "ridecare/upload_rule/snapshot"
    RC_IMU_VIDEO = "ridecare/imu/video"
    RC_PIPELINE_ANON_VIDEO = "ridecare/pipeline/anonymize/video"
    RC_PIPELINE_ANON_SNAPSHOT = "ridecare/pipeline/anonymize/snapshot"
    RC_PIPELINE_CHC_VIDEO = "ridecare/pipeline/chc/video"
    RC_PIPELINE_STATUS = "ridecare/pipeline/status"
    RC_OPERATOR = "ridecare/operator"
    RC_EVENT = "ridecare/event"

    def __str__(self) -> str:
        """
        Return the name of the field (removes the need of calling .value)

        Returns:
            str: The field name
        """
        return str.__str__(self)


@inject
class RequestFactory:
    """ Request Factory """

    def __init__(self, config: ArtifactDownloaderConfig, s3_downloader: S3Downloader):
        """
        Constructor
        """
        self.__config = config
        self.__s3_downloader = s3_downloader

    def generate_request_from_artifact_with_file(self,
                                                 endpoint: PartialEndpoint,
                                                 message: ConfiguredBaseModel,
                                                 s3_path: str) -> Request:
        """
        Download json data and returns a post request with the following structure:
        {message=message,data="downloaded_data"}

        Where downloaded_data is the file pointer by "s3_path". Only JSON data is accepted!

        Args:
            endpoint (PartialEndpoints): The endpoint to be used
            message (Union[Artifact, ProcessingResult]): The artifact to be sent
            s3_path (str): The s3 path to download data from

        Returns:
            Request: _description_
        """
        message_artifact = message.stringify()
        data = self.__s3_downloader.download_convert_json(s3_path)

        url = str(self.__config.artifact_base_url) + str(endpoint)
        request = Request("POST", url, data={"data": data, "message": message_artifact})
        return request

    def generate_request_from_artifact(self, endpoint: PartialEndpoint,
                                       data: ConfiguredBaseModel) -> Request:
        """
        Returns the request form an artifact

        Args:
            endpoint (PartialEndpoints): The endpoint to be used
            data (Union[Artifact, ProcessingResult]): The artifact to be sent

        Returns:
            Request: _description_
        """
        url = str(self.__config.artifact_base_url) + str(endpoint)
        request = Request("POST", url, data=data.stringify())
        return request

    def generate_request(self, endpoint: PartialEndpoint, data: str) -> Request:
        """
        Returns the request with the base path appended

        Args:
            endpoint (Endpoint): The endpoint to be used
            data (str): Raw data for request
        Returns:
            Request: The request to be made
        """
        url = str(self.__config.artifact_base_url) + str(endpoint)
        request = Request("POST", url, data=data)
        return request
