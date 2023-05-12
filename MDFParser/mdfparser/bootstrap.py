# type: ignore
# pylint: disable=E1120
"""bootstrap dependency injection autowiring."""
import os
from dataclasses import dataclass
from logging import Logger

from mypy_boto3_sqs import SQSClient
from mypy_boto3_s3 import S3Client

from base.aws.container_services import ContainerServices
from base.chc_counter import ChcCounter
from base.gnss_coverage import GnssCoverage
from base.max_audio_loudness import MaxAudioLoudness
from base.max_person_count import MaxPersonCount
from base.mean_audio_bias import MeanAudioBias
from base.median_person_count import MedianPersonCount
from base.ride_detection_people_count_before import RideDetectionPeopleCountBefore
from base.ride_detection_people_count_after import RideDetectionPeopleCountAfter
from base.sum_door_closed import SumDoorClosed
from base.variance_person_count import VariancePersonCount

import boto3
from kink import di
from mdfparser.config import MdfParserConfig
from mdfparser.consumer import Consumer
from mdfparser.imu.downloader import IMUDownloader
from mdfparser.imu.handler import IMUHandler
from mdfparser.imu.transformer import IMUTransformer
from mdfparser.imu.uploader import IMUUploader
from mdfparser.metadata.downloader import MetadataDownloader
from mdfparser.metadata.handler import MetadataHandler
from mdfparser.metadata.synchronizer import Synchronizer
from mdfparser.metadata.uploader import MetadataUploader


@dataclass
class EnvironmentParams:
    """Environment parameters."""
    container_version: str
    config_path: str
    container_name: str
    region_name: str


def get_environment() -> EnvironmentParams:
    """Returns environment parameters."""
    container_version = os.getenv("CONTAINER_VERSION", "development")
    config_path = os.getenv("CONFIG_PATH", "/app/config/config.yml")
    container_name = os.getenv("CONTAINER_NAME", "MDFParser")
    region_name = os.getenv("REGION_NAME", "eu-central-1")

    return EnvironmentParams(
        container_version=container_version,
        config_path=config_path,
        container_name=container_name,
        region_name=region_name
    )


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""
    env_params = get_environment()
    di[Logger] = ContainerServices.configure_logging("mdfparser")

    di[SQSClient] = boto3.client("sqs", region_name=env_params.region_name)
    di[S3Client] = boto3.client("s3", region_name=env_params.region_name)
    di["container_version"] = env_params.container_version
    di["container_name"] = env_params.container_name
    di["config_path"] = env_params.config_path
    di["processor_list"] = [
        ChcCounter(),
        GnssCoverage(),
        MaxAudioLoudness(),
        MaxPersonCount(),
        MeanAudioBias(),
        MedianPersonCount(),
        VariancePersonCount(),
        RideDetectionPeopleCountBefore(),
        RideDetectionPeopleCountAfter(),
        SumDoorClosed()
    ]
    di[ContainerServices] = ContainerServices(container="MDFParser", version=env_params.container_version)
    di[MdfParserConfig] = MdfParserConfig.load_config_from_yaml_file(di["config_path"])

    di[MetadataDownloader] = MetadataDownloader()
    di[MetadataUploader] = MetadataUploader()
    di[Synchronizer] = Synchronizer()

    di[IMUTransformer] = IMUTransformer()
    di[IMUDownloader] = IMUDownloader()
    di[IMUUploader] = IMUUploader()

    di[Consumer] = Consumer(handler_list=[IMUHandler(), MetadataHandler()])
