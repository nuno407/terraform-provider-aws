""" artifact controller module. """
from kink import inject

from sanitizer.artifact.artifact_filter import ArtifactFilter
from sanitizer.artifact.artifact_forwarder import ArtifactForwarder
from sanitizer.artifact.artifact_injector import MetadataArtifactInjector
from sanitizer.artifact.artifact_parser import ArtifactParser


@inject
class ArtifactController:  # pylint: disable=too-few-public-methods
    """ Artifact controller class. """

    def __init__(self,
                 parser: ArtifactParser,
                 afilter: ArtifactFilter,
                 forwarder: ArtifactForwarder,
                 injector: MetadataArtifactInjector):
        self.parser = parser
        self.filter = afilter
        self.forwarder = forwarder
        self.injector = injector
