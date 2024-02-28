import logging
from kink import inject
from base.mongo.engine import Engine
from base.model.artifacts import (
    CameraBlockedOperatorArtifact,
    PeopleCountOperatorArtifact,
    SOSOperatorArtifact,
    OtherOperatorArtifact)
from artifact_api.exceptions import InvalidOperatorArtifactException

from artifact_api.exceptions import InvalidOperatorArtifactException

_logger = logging.getLogger(__name__)


@inject
class MongoSavOperatorService():

    def __init__(self, operator_feedback_engine: Engine):
        """
        Constructor

        Args:
            operator_feedback_engine (Engine): operator feedback engine
        """
        self.__operator_feedback_engine = operator_feedback_engine

    async def save_event(self, artifact: SOSOperatorArtifact | PeopleCountOperatorArtifact | CameraBlockedOperatorArtifact | OtherOperatorArtifact):
        """
        Create operator feedback entry in database
        Args:
            artifact: The artifact to store
        """

        if isinstance(
            artifact,
            (SOSOperatorArtifact,
             PeopleCountOperatorArtifact,
             CameraBlockedOperatorArtifact,
             OtherOperatorArtifact)):
            await self.__operator_feedback_engine.save(artifact)
            _logger.debug(
                "Operator message saved to db [%s]", artifact.model_dump_json())
        else:
            raise InvalidOperatorArtifactException()
