
from abc import ABCMeta, abstractmethod
from base.aws.container_services import ContainerServices


class RCCArtifact(ABCMeta):
    "This class acts as an interface and shall have common functions between the artifact types to decrease code duplication"
    ...

    @staticmethod
    def check_s3_anonymized_file_presence(file_key: str, container_services: ContainerServices) -> bool:
        ...

    @staticmethod
    def check_s3_raw_file_presence(file_key: str, container_services: ContainerServices) -> bool:
        ...

    @staticmethod
    def check_voxel_file_presence(file_key: str, container_services: ContainerServices) -> bool:
        ...

    @staticmethod
    def set_data_status_incomplete(artifact_id: str, container_services: ContainerServices):
        ...

    @abstractmethod
    def is_artifact_processed(self) -> bool:
        raise NotImplementedError("This method needs to be implemented")

    @abstractmethod
    def is_health_good(self) -> tuple[bool, str]:
        raise NotImplementedError("This method needs to be implemented")


class TrainingRecorderArtifact(RCCArtifact):
    def __init__(self, artifact_hash: str, container_services: ContainerServices):
        self.video_id: str = "None"
        self.container_services = None

    @classmethod
    def from_pipeline_id(cls, pipeline_id: str, container_services: ContainerServices):
        ob = cls.__new__(cls)
        ob.video_id = pipeline_id

    def is_artifact_processed(self) -> bool:
        ...

    def is_health_good(self) -> tuple[bool, str]:
        ...

    def __preform_db_checks(self) -> bool:
        ...


class InteriorRecorderArtifact(RCCArtifact):
    def __init__(self, artifact_hash: str, container_services: ContainerServices):
        self.video_id: str = "None"
        self.container_services = None

    @classmethod
    def from_pipeline_id(cls, pipeline_id: str, container_services: ContainerServices):
        ob = cls.__new__(cls)
        ob.video_id = pipeline_id

        return ob

    def is_artifact_processed(self):
        ...

    def is_health_good(self) -> tuple[bool, str]:
        ...

    def __preform_db_checks(self) -> bool:
        ...


class SnpashotArtifact(RCCArtifact):
    def __init__(self, snapshot_id: str, container_services: ContainerServices):
        self.snapshot_id = snapshot_id
        self.container_services = None

    def is_artifact_processed(self) -> bool:
        ...

    def is_health_good(self) -> tuple[bool, str]:
        ...

    def __preform_db_checks(self) -> bool:
        ...
