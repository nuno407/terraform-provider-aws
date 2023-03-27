""" artifact filter module. """
from sanitizer.model import Artifact

class ArtifactFilter:
    """ Artifact filter class. """
    def apply(self, artifact: list[Artifact]) -> list[Artifact]:
        raise NotImplementedError("TODO")
