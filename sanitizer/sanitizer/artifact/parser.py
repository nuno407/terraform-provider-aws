""" Artifact Parser module. """
from kink import inject

from base.aws.model import SQSMessage
from sanitizer.artifact.persistence import ArtifactPersistence


@inject
class ArtifactParser:
    def __init__(self, artifact_persistence: ArtifactPersistence) -> None:
        self.artifact_persistence = artifact_persistence

    def parse(self, raw_message: str) -> SQSMessage:
        raise NotImplementedError("TODO")
