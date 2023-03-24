""" ingestor module """
import hashlib
import json
import logging as log
import os
import subprocess
import tempfile
from datetime import timedelta
from typing import Optional

from sdretriever.ingestor.ingestor import Ingestor
from sdretriever.exceptions import FileAlreadyExists
from sdretriever.message.video import VideoMessage

from base.timestamps import from_epoch_seconds_or_milliseconds

LOGGER = log.getLogger("SDRetriever." + __name__)


class VideoIngestor(Ingestor):
    """ Video ingestor class """

    def __init__(
            self,
            container_services,
            s3_client,
            sqs_client,
            sts_helper,
            discard_repeated_video: bool,
            frame_buffer=0) -> None:
        super().__init__(container_services, s3_client, sqs_client, sts_helper)
        LOGGER.info("Loading configuration for video ingestor discard_repeated_video=%s",
                    str(discard_repeated_video))
        self.clip_ext = ".mp4"
        self.stream_timestamp_type = 'PRODUCER_TIMESTAMP'
        self.frame_buffer = frame_buffer
        self.discard_repeated_video = discard_repeated_video

    @ staticmethod
    def _ffmpeg_probe_video(video_bytes) -> dict:
        # On completion of the context or destruction of the temporary directory object,
        # the newly created temporary directory and all its
        # contents are removed from the filesystem.
        with tempfile.TemporaryDirectory() as auto_cleaned_up_dir:

            # Store bytes into current working directory as video
            temp_video_file = os.path.join(
                auto_cleaned_up_dir, 'input_video.mp4')
            with open(temp_video_file, "wb") as f:
                f.write(video_bytes)

            # Execute ffprobe command to get video clip info
            result = subprocess.run(["/usr/bin/ffprobe",  # nosec
                                    "-v",
                                     "error",
                                     "-show_format",
                                     "-show_streams",
                                     "-print_format",
                                     "json",
                                     temp_video_file],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    check=False)

            # Load ffprobe output (bytes) as JSON
            decoded_info = (result.stdout).decode("utf-8")
            video_info = json.loads(decoded_info)
            return video_info

    def ingest(
            self,
            video_msg: VideoMessage,
            training_whitelist: list[str] = [],
            request_training_upload=True) -> Optional[dict]:
        """Obtain video from KinesisVideoStreams and upload it to our raw data S3"""
        # Define the target path where to place the video
        s3_folder = str(video_msg.tenant) + "/"

        # Get clip from KinesisVideoStream
        try:
            # Requests credentials to assume specific cross-account role on Kinesis
            role_credentials = self.sts_helper.get_credentials()
            video_from = from_epoch_seconds_or_milliseconds(
                video_msg.footagefrom)
            video_to = from_epoch_seconds_or_milliseconds(video_msg.footageto)
            seed = f"{video_msg.streamname}_{int(video_from.timestamp() * 1000)}_{int(video_to.timestamp() * 1000)}"

            internal_message_reference_id = hashlib.sha256(
                seed.encode("utf-8")).hexdigest()
            LOGGER.info("internal_message_reference_id generated hash=%s seed=%s",
                        internal_message_reference_id, seed)

            video_bytes, video_start_ts, video_end_ts = self.container_svcs.get_kinesis_clip(
                role_credentials, video_msg.streamname, video_from, video_to, self.stream_timestamp_type)
            video_start = round(video_start_ts.timestamp() * 1000)
            video_end = round(video_end_ts.timestamp() * 1000)
        except Exception as exception:
            LOGGER.exception(
                f"Could not obtain Kinesis clip from stream {video_msg.streamname} between {repr(video_from)} and {repr(video_to)} -> {repr(exception)}",
                exception,
                extra={
                    "messageid": video_msg.messageid})
            return None

        # Upload video clip into raw data S3 bucket
        s3_filename = f"{video_msg.streamname}_{video_start}_{video_end}"
        s3_path = s3_folder + s3_filename + self.clip_ext

        if self.discard_repeated_video:
            LOGGER.info("Checking for the existance of %s file in the %s bucket", s3_path, self.container_svcs.raw_s3)
            exists_on_devcloud = self.container_svcs.check_s3_file_exists(
                self.s3_client, self.container_svcs.raw_s3, s3_path)

            if exists_on_devcloud:
                raise FileAlreadyExists(f"Video {s3_path} already exists on DevCloud, message will be skipped")

        try:
            self.container_svcs.upload_file(self.s3_client, video_bytes,
                                            self.container_svcs.raw_s3, s3_path)
            LOGGER.info(f"Successfully uploaded to {self.container_svcs.raw_s3}/{s3_path}", extra={
                        "messageid": video_msg.messageid})
        except Exception as exception:
            LOGGER.error(f"Could not upload file to {s3_path} -> {repr(exception)}", extra={
                         "messageid": video_msg.messageid})
            raise exception

        # Obtain video details via ffprobe and prepare data to be used
        # to generate the video's entry on the database

        video_info = self._ffmpeg_probe_video(video_bytes)

        # Calculate video information
        width = str(video_info["streams"][0]["width"])
        height = str(video_info["streams"][0]["height"])
        video_seconds = round(float(video_info["format"]["duration"]))

        # Build the cedord data to be sent into DB
        db_record_data = {
            "_id": s3_filename,
            "MDF_available": "No",
            "media_type": "video",
            "s3_path": self.container_svcs.raw_s3 + "/" + s3_path,
            "footagefrom": video_start,
            "footageto": video_end,
            "tenant": video_msg.tenant,
            "deviceid": video_msg.deviceid,
            'length': str(timedelta(seconds=video_seconds)),
            '#snapshots': str(0),
            'snapshots_paths': [],
            "sync_file_ext": "",
            'resolution': width + "x" + height,
            "internal_message_reference_id": internal_message_reference_id,
        }

        # Generate dictionary with info to send to Selector container for training data request (high quality )
        is_interior_recording = video_msg.video_recording_type() == 'InteriorRecorder'
        if is_interior_recording and request_training_upload and video_msg.tenant in training_whitelist:
            hq_request = {
                "streamName": f"{video_msg.tenant}_{video_msg.deviceid}_InteriorRecorder",
                "deviceId": video_msg.deviceid,
                "footageFrom": video_msg.footagefrom - self.frame_buffer,
                "footageTo": video_msg.footageto + self.frame_buffer
            }
            # Send message to secondary input queue of Selector container
            hq_selector_queue = self.container_svcs.sqs_queues_list["HQ_Selector"]
            self.container_svcs.send_message(
                self.sqs_client, hq_selector_queue, hq_request)

        return db_record_data
