""" post processor module """
import json
import os
import subprocess  # nosec
import tempfile
from dataclasses import dataclass
from typing import Protocol

from kink import inject


@dataclass
class VideoInfo:
    """ Video information """
    duration: float
    width: int
    height: int


class IVideoPostProcessor(Protocol):  # pylint: disable=too-few-public-methods
    """ Post processing interface """

    def execute(self, video_bytes: bytes) -> VideoInfo:
        """ execute post procesing """


@inject(alias=IVideoPostProcessor)
class FFProbeExtractorPostProcessor:  # pylint: disable=too-few-public-methods
    """ Runs ffmpeg to extract video information in json format """

    def execute(self, video_bytes: bytes) -> VideoInfo:
        """ executes post-processor """
        # On completion of the context or destruction of the temporary directory object,
        # the newly created temporary directory and all its
        # contents are removed from the filesystem.
        with tempfile.TemporaryDirectory() as auto_cleaned_up_dir:
            # Store bytes into current working directory as video
            temp_video_file = os.path.join(
                auto_cleaned_up_dir, "input_video.mp4")
            with open(temp_video_file, "wb") as f_p:
                f_p.write(video_bytes)

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

            # Extract video information from output
            width = video_info["streams"][0]["width"]
            height = video_info["streams"][0]["height"]
            video_seconds = float(video_info["format"]["duration"])
            return VideoInfo(video_seconds, width, height)
