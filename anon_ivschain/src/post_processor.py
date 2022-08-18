import logging
import subprocess
import os

from baseaws.shared_functions import AWSServiceClients, ContainerServices, VIDEO_FORMATS



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
        file_format = file_format.replace(".","")

        if file_format in VIDEO_FORMATS:
            logging.info("Starting conversion (AVI to MP4) process..\n")
            mp4_path = path + ".mp4"
            logs_path = path.split("_Anonymize")[0] + "_conversion_logs.txt"

            try:
                # Download target file to be converted
                avi_video = self.container_services.download_file(
                    self.aws_clients.s3_client, self.container_services.anonymized_s3, media_path)

                # Store input video file into current working directory
                with open(self.INPUT_NAME, "wb") as input_file:
                    input_file.write(avi_video)

                with open(self.LOGS_NAME, 'w') as logs_write:
                    # Convert .avi input file into .mp4 using ffmpeg
                    conv_logs = subprocess.Popen(["ffmpeg", "-i", self.INPUT_NAME, "-movflags", "faststart", "-c:v", "copy", self.OUTPUT_NAME],
                                                 stdout=subprocess.PIPE,
                                                 stderr=subprocess.STDOUT,
                                                 universal_newlines=True)
                
                    # Save conversion logs into txt file
                    for line in conv_logs.stdout:
                        logs_write.write(line)

                # Load bytes from converted output file
                with open(self.OUTPUT_NAME, "rb") as output_file:
                    output_video = output_file.read()

                logging.info("\nConversion complete!\n")
                # Upload converted output file to S3 bucket
                self.container_services.upload_file(
                    self.aws_clients.s3_client, output_video, self.container_services.anonymized_s3, mp4_path)

                # Load bytes from logs file
                with open(self.LOGS_NAME, "rb") as logs_bytes:
                    logs_file = logs_bytes.read()

                # Upload conversion logs to S3 bucket
                self.container_services.upload_file(
                    self.aws_clients.s3_client, logs_file, self.container_services.anonymized_s3, logs_path)

            except Exception as err:
                logging.exception(f"Error during post-processing: {err}")
            finally:
                subprocess.run(["rm", self.INPUT_NAME, self.OUTPUT_NAME, self.LOGS_NAME])
        
        else:
            logging.error(f"File format {file_format} is unknown")
