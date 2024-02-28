"""CHC container handler"""
from datetime import timedelta

from requests import Request
from kink import inject

from base.model.artifacts import CHCResult
from base.model.artifacts.api_messages import CHCDataResult
from artifact_downloader.chc_synchronizer import ChcSynchronizer
from artifact_downloader.post_processor import IVideoPostProcessor
from artifact_downloader.s3_downloader import S3Downloader
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import CHCMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint


@inject
class CHCContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """CHC container handler"""

    def __init__(self, request_factory: RequestFactory, s3_downloader: S3Downloader,
                 post_processor: IVideoPostProcessor, chc_syncronizer: ChcSynchronizer):
        """
        Constructor

        Args:
            request_factory (RequestFactory): request_factory
            s3_downloader (S3Downloader): s3_downloader
            post_processor (IVideoPostProcessor): post_processor
        """
        self.__request_factory = request_factory
        self.__s3_downloader = s3_downloader
        self.__post_processor = post_processor
        self.__endpoint_video = PartialEndpoint.RC_PIPELINE_CHC_VIDEO
        self.__chc_syncronizer = chc_syncronizer

    def create_request(self, message: CHCMessage) -> Request:
        """
        A CHC message

        Args:
            message (CHCMessage): _description_

        Raises:
            UnexpectedContainerMessage: _description_

        Returns:
            Request: _description_
        """
        if not isinstance(message.body, CHCResult):
            raise UnexpectedContainerMessage(f"Message of type {type(message.body)} is not chc message")

        if message.body.raw_s3_path.endswith(".mp4"):
            signals = self.download_and_synchronize_chc(message.body.s3_path, message.body.raw_s3_path)
            chc_data = CHCDataResult(message=message.body, data=signals)
            return self.__request_factory.generate_request_from_artifact(self.__endpoint_video, chc_data)

        raise UnexpectedContainerMessage("Anonymization result is not a video")

    def download_and_synchronize_chc(self, chc_s3_path: str, video_s3_path: str) -> dict:
        """Downloads and synchronize CHC signals based on recording length.

        Args:
            chc_s3_path (str): S3 path of the chc signals file
            video_s3_path (str): S3 path of the video file
        Returns:
            Parsed CHC signals and recording overview information.
        """
        # get video length from the original video
        video_file = self.__s3_downloader.download(video_s3_path)
        video_info = self.__post_processor.execute(video_file)

        video_length = timedelta(seconds=video_info.duration)

        # do the synchronisation
        chc_dict = self.__s3_downloader.download_convert_json(chc_s3_path)
        chc_sync = self.__chc_syncronizer.synchronize(chc_dict, video_length)

        return chc_sync
