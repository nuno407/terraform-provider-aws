""" artifact persistence module. """
from pymongo import MongoClient

from sanitizer.model import Artifact


class ArtifactPersistence:
    """ Artifact persistence class. """

    def __init__(self, mongo_client: MongoClient) -> None:
        self.mongo_client = mongo_client

    def save(self, artifact: Artifact) -> None:
        """ Saves given artifact in database collection """
        raise NotImplementedError("TODO")
