import logging
import os
import subprocess
from subprocess import CalledProcessError

from baseaws.shared_functions import (VIDEO_FORMATS, AWSServiceClients,
                                      ContainerServices)

_logger = ContainerServices.configure_logging('AnonymizePostProcessor')

class AnonymizePostProcessor():
    """ AnonymizePostProcessor """

    # Defining temporary files names constants
    INPUT_NAME = "input_video.avi"
    OUTPUT_NAME = "output_video.mp4"
    LOGS_NAME = "logs.txt"

    def __init__(self, container_services: ContainerServices, aws_clients: AWSServiceClients) -> None:
        '''
        Creates a post processor to convert the videos with ffmpeg when calling the run() method

        Args:
            container_services (ContainerServices): container services instance
            aws_clients (AWSServiceClients): aws boto3 clients wrapper
        '''
        self.container_services = container_services
        self.aws_clients = aws_clients

    def run(self, message_body: dict) -> None:
        '''
        Execute post processing using ffmpeg to convert the video files from AVI to MP4

        Args:
            message_body (dict): request body with the information
        '''

        media_path = message_body['media_path']
        path, file_format = os.path.splitext(media_path)
        file_format = file_format.replace(".", "")

        if file_format in VIDEO_FORMATS:
            _logger.info("Starting conversion (AVI to MP4) process..\n")
            mp4_path = path + ".mp4"

            try:
                # Download target file to be converted
                avi_video = self.container_services.download_file(
                    self.aws_clients.s3_client, self.container_services.anonymized_s3, media_path)

                # Store input video file into current working directory
                with open(self.INPUT_NAME, "wb") as input_file:
                    input_file.write(avi_video)

                stat_result = os.stat(self.INPUT_NAME)

                _logger.debug(f'Anonymized artifact downloaded file size: {stat_result.st_size}')

                # Convert .avi input file into .mp4 using ffmpeg
                ffmpeg_command = ' '.join(["ffmpeg", "-i", self.INPUT_NAME, "-movflags", "faststart", "-c:v", "copy", self.OUTPUT_NAME])

                _logger.info('Starting ffmpeg process')
                try:
                    subprocess.check_call(ffmpeg_command, shell=True, executable='/bin/sh')
                except CalledProcessError as err:
                    print(f'Error converting file with ffmpeg: {err.returncode}')
                    exit(1)

                _logger.info("Conversion complete!")

                # Load bytes from converted output file
                with open(self.OUTPUT_NAME, "rb") as output_file:
                    output_video = output_file.read()

                # Upload converted output file to S3 bucket
                self.container_services.upload_file(
                    self.aws_clients.s3_client, output_video, self.container_services.anonymized_s3, mp4_path)

            except Exception as err:
                _logger.exception(f"Error during post-processing: {err}")

        else:
            _logger.error(f"File format {file_format} is unknown")
