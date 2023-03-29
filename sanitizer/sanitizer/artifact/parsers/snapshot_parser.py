from base.aws.model import SQSMessage
from base.model.artifacts import SnapshotArtifact
from sanitizer.artifact.parser import ArtifactParser


class SnapshotParser(ArtifactParser):
    def __init__(self) -> None:
        pass

    def parse(self, sqs_message: SQSMessage) -> list[SnapshotArtifact]:
        raise NotImplementedError("SnapshotParser.parse() not implemented yet")
