"""IMU Handler"""
import pandas as pd
from kink import inject
from mdfparser.config import MdfParserConfig
from mdfparser.constants import IMU_TMP_SUFFIX
from mdfparser.imu.downloader import IMUDownloader
from mdfparser.imu.transformer import IMUTransformer
from mdfparser.imu.uploader import IMUUploader
from mdfparser.interfaces.handler import Handler
from mdfparser.interfaces.input_message import DataType, InputMessage
from mdfparser.interfaces.output_message import OutputMessage


@inject
class IMUHandler(Handler):
    """
    The IMUHandler used to process IMU messages.
    """

    def __init__(
            self,
            downloader: IMUDownloader,
            uploader: IMUUploader,
            transformer: IMUTransformer,
            config: MdfParserConfig):
        """
        Initializes the IMUHandler.

        Args:
            downloader (IMUDownloader): _description_
            uploader (IMUUploader): _description_
            transformer (IMUTransformer): _description_
            config (MdfParserConfig): _description_
        """
        self.downloader = downloader
        self.uploader = uploader
        self.transformer = transformer
        self.config = config

    def ingest(self, message: InputMessage) -> OutputMessage:
        """
        Process the IMU data.

        Args:
            message (InputMessage): The message coming from the queue.

        Returns:
            OutputMessage: The message to be sent to the metadata queue.
        """

        # Download the IMU data
        imu_data: pd.DataFrame = self.downloader.download(message.s3_path)

        # Apply necessary transformations
        processed_imu: pd.DataFrame = self.transformer.apply_transformation(imu_data)

        # Free memory
        del imu_data

        # Apply source information to every field
        self.__apply_source_info(processed_imu, message)

        # Upload the file
        tmp_file_key = f"{message.id}{IMU_TMP_SUFFIX}"
        self.uploader.upload(processed_imu, self.config.temporary_bucket, tmp_file_key)

        return OutputMessage(
            message.id, f"s3://{self.config.temporary_bucket}/{tmp_file_key}", DataType.IMU, {})

    def handler_type(self) -> DataType:
        """
        This handler shall process IMU messages.

        Returns:
            DataType: imu
        """
        return DataType.IMU

    def __apply_source_info(self, imu_parsed: pd.DataFrame, input_message: InputMessage) -> None:
        """
        Adds the source info (tenant, device_id, recorder) to each IMU document.

        Args:
            imu_parsed (pd.DataFrame): The parsed IMU.
            input_message (InputMessage): The input message used to get the source information.
        """
        imu_parsed["source"] = [{
            "device_id": input_message.device_id,
            "tenant": input_message.tenant}] * len(imu_parsed)
