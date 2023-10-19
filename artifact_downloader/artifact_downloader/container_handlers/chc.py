"""CHC container handler"""
import os
from datetime import timedelta

from requests import Request
from kink import inject

from artifact_downloader.chc_synchronizer import ChcSynchronizer
from artifact_downloader.post_processor import IVideoPostProcessor
from artifact_downloader.s3_downloader import S3Downloader
from base.model.artifacts import CHCResult
from artifact_downloader.exceptions import UnexpectedContainerMessage
from artifact_downloader.message.incoming_messages import CHCMessage
from artifact_downloader.container_handlers.handler import ContainerHandler
from artifact_downloader.request_factory import RequestFactory, PartialEndpoint
from base.model.metadata.api_messages import CHCDataResult


@inject
class CHCContainerHandler(ContainerHandler):  # pylint: disable=too-few-public-methods
    """CHC container handler"""

    def __init__(self, request_factory: RequestFactory, s3_downloader: S3Downloader,
                 post_processor: IVideoPostProcessor):
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
            recording_id = os.path.basename(message.body.raw_s3_path).split(".")[0]
            chc_data = CHCDataResult(id=recording_id, chc_path=message.body.raw_s3_path, data=signals)
            return self.__request_factory.generate_request_from_artifact(self.__endpoint_video, chc_data)

        raise UnexpectedContainerMessage("Anonymization result is neither a snapshot nor video")

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

        video_length = timedelta(video_info.duration)

        # do the synchronisation
        chc_syncer = ChcSynchronizer()
        chc_dict = self.__s3_downloader.download_convert_json(chc_s3_path)
        chc_sync = chc_syncer.synchronize(chc_dict, video_length)
        chc_sync_parsed = {str(ts): signals for ts, signals in chc_sync.items()}

        return chc_sync_parsed
