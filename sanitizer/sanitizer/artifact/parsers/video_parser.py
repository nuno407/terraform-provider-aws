from base.aws.model import SQSMessage
from base.model.artifacts import VideoArtifact
from sanitizer.artifact.parser import ArtifactParser


class VideoParser(ArtifactParser):
    def __init__(self) -> None:
        pass

    def parse(self, sqs_message: SQSMessage) -> list[VideoArtifact]:
        raise NotImplementedError("VideoParser.parse() not implemented yet")
