from typing import Union
from dataclasses import dataclass
from base.model.artifacts import ProcessingResult, Artifact, S3Path
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.config import ArtifactDownloaderConfig
from kink import inject
from requests import Request
from enum import Enum
import os

@dataclass
class PartialEndpoints(Enum):
    RC_SIGNALS_VIDEO="/ridecare/signals/video"
    RC_SIGNALS_SNAPSHOT="/ridecare/signals/snapshot"
    RC_VIDEO="/ridecare/video"
    RC_SNAPSHOT="/ridecare/snapshots"
    RC_IMU_VIDEO="/ridecare/imu/video"
    RC_PIPELINE_ANON_VIDEO="/ridecare/pipeline/anonymize/video"
    RC_PIPELINE_ANON_SNAPSHOT="/ridecare/pipeline/anonymize/snapshot"
    RC_PIPELINE_CHC_VIDEO="/ridecare/pipeline/chc/video"
    RC_PIPELINE_CHC_SNAPSHOT="/ridecare/pipeline/chc/snapshot"
    RC_PIPELINE_STATUS="/ridecare/pipeline/chc/status"
    RC_OPERATOR="/ridecare/operator"
    RC_EVENT="/ridecare/event"

@inject
class RequestFactory:

    def __init__(self, config: ArtifactDownloaderConfig, s3_downloader: S3Downloader):
        """
        Constructor
        """
        self.__config = config
        self.__s3_downloader = s3_downloader


    def generate_request_from_artifact_with_file(self, endpoint: PartialEndpoints, message: Union[Artifact, ProcessingResult], s3_path: str) -> Request:
        """
        Download json data and returns a post request with the following structure:
        {message=message,data="downloaded_data"}

        Where downloaded_data is the file pointer by "s3_path". Only JSON data is acceped!

        Args:
            endpoint (PartialEndpoints): The endpoint to be used
            data (Union[Artifact, ProcessingResult]): The artifact to be sent
            s3_path (str): The s3 path to download data from

        Returns:
            Request: _description_
        """
        message_artifact = message.stringify()
        data = self.__s3_downloader.download_convert_json(s3_path)

        url = os.path.join(self.__config.artifact_base_url, endpoint)
        request = Request("POST", url, data={"data":data,"message":message_artifact})
        return request


    def generate_request_from_artifact(self, endpoint: PartialEndpoints, data: Union[Artifact, ProcessingResult]) -> Request:
        """
        Returns the request form an artifact

        Args:
            endpoint (PartialEndpoints): The endpoint to be used
            data (Union[Artifact, ProcessingResult]): The artifact to be sent

        Returns:
            Request: _description_
        """
        url = os.path.join(self.__config.artifact_base_url, endpoint)
        request = Request("POST", url, data=data.stringify())
        return request

    def generate_request(self, endpoint: PartialEndpoints, data: str) -> Request:
        """
        Returns the request with the base path appended

        Args:
            endpoint (Endpoint): The endpoint to be used
        Returns:
            Request: The request to be made
        """
        url = os.path.join(self.__config.artifact_base_url, endpoint)
        request = Request("POST", url, data=data)
        return request


