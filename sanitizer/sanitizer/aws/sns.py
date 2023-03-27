from kink import inject
from mypy_boto3_sns import SNSClient

from sanitizer.config import SanitizerConfig
from sanitizer.model import Artifact


@inject
class AWSSNSController:
    def __init__(self,
                config: SanitizerConfig,
                sns_client: SNSClient):
        self.__config = config
        self.__sns_client = sns_client

    def publish(self, artifact: Artifact) -> None:
        raise NotImplementedError("TODO")
