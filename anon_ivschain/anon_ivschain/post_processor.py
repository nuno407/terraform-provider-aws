import logging
import os
import subprocess  # nosec
from subprocess import CalledProcessError  # nosec

from base.aws.container_services import ContainerServices
from base.aws.shared_functions import AWSServiceClients
from base.constants import IMAGE_FORMATS, VIDEO_FORMATS
from basehandler.message_handler import OperationalMessage

_logger: logging.Logger = ContainerServices.configure_logging("AnonymizePostProcessor")

class AnonymizePostProcessor():
    """ AnonymizePostProcessor """

    # Defining temporary files names constants
    INPUT_NAME = "input_video.avi"
    OUTPUT_NAME = "output_video.mp4"
    LOGS_NAME = "logs.txt"

    def __init__(self, container_services: ContainerServices, aws_clients: AWSServiceClients, mocked: bool = False) -> None:
        '''
        Creates a post processor to convert the videos with ffmpeg when calling the run() method

        Args:
            container_services (ContainerServices): container services instance
            aws_clients (AWSServiceClients): aws boto3 clients wrapper
            mocked (bool): mocked
        '''
        self.container_services = container_services
        self.aws_clients = aws_clients
        self.mocked = mocked

    def _mocked_execution(self, ffmpeg_command: str) -> None:
        """mocked_execution.

        Mocks ffmpeg command execution for when receiving a mocked response from ivsfc.

        Args:
            ffmpeg_command (str): ffmpeg command line with arguments for logging
        """
        try:
            _logger.info("Mocking: %s", ffmpeg_command)
            with open(self.INPUT_NAME, "rb") as input_file:
                input_file_content = input_file.read()
            _logger.info("Writting mocked: %s", self.OUTPUT_NAME)
            with open(self.OUTPUT_NAME, "wb") as output_file:
                output_file.write(input_file_content)
        except OSError as err:
            _logger.error("Error mocking ffmpeg %s", err)
        finally:
            os.remove(self.INPUT_NAME)

    def run(self, message_body: OperationalMessage) -> None:
        '''
        Execute post processing using ffmpeg to convert the video files from AVI to MP4

        Args:
            message_body (dict): request body with the information
        '''

        media_path = message_body.media_path
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

                _logger.debug(f"Anonymized artifact downloaded file size: {stat_result.st_size}")

                # Convert .avi input file into .mp4 using ffmpeg
                ffmpeg_command = " ".join(["/usr/bin/ffmpeg", "-y", "-i", self.INPUT_NAME, "-movflags",
                                          "faststart", "-c:v", "copy", self.OUTPUT_NAME])

                _logger.info("Starting ffmpeg process")
                if self.mocked:
                    self._mocked_execution(ffmpeg_command)
                else:
                    subprocess.check_call(ffmpeg_command, shell=True, executable="/bin/sh")  # nosec

                _logger.info("Conversion complete!")

                # Load bytes from converted output file
                with open(self.OUTPUT_NAME, "rb") as output_file:
                    output_video = output_file.read()

                # Upload converted output file to S3 bucket
                self.container_services.upload_file(
                    self.aws_clients.s3_client, output_video, self.container_services.anonymized_s3, mp4_path)
            except CalledProcessError as err:
                print(f'Error converting file with ffmpeg: {err.returncode}')
                exit(1)
            except Exception as err:
                _logger.exception(f"Error during post-processing: {err}")
                exit(1)
            finally:
                # Remove conversion video file
                os.remove(self.OUTPUT_NAME)
        elif file_format in IMAGE_FORMATS:
            _logger.info("No conversion needed, artifact is an image")
        else:
            _logger.error(f"File format {file_format} is unknown")
