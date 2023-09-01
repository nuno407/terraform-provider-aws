"""Module for determining the correct checker for a given artifact"""
from kink import inject

from base.model.artifacts import (Artifact, OperatorArtifact, RecorderType,
                                  SnapshotArtifact, VideoArtifact)
from healthcheck.checker.interior_recorder import \
    InteriorRecorderArtifactChecker
from healthcheck.checker.sav_operator_events import SAVOperatorArtifactChecker
from healthcheck.checker.snapshot import SnapshotArtifactChecker
from healthcheck.checker.training_recorder import \
    TrainingRecorderArtifactChecker


@inject
class CheckerDeterminer:
    """Class for determining the correct checker for a given artifact"""

    def __init__(self,
                 training_checker: TrainingRecorderArtifactChecker,
                 interior_checker: InteriorRecorderArtifactChecker,
                 snapshot_checker: SnapshotArtifactChecker,
                 operator_checker: SAVOperatorArtifactChecker) -> None:
        self.__training_checker = training_checker
        self.__interior_checker = interior_checker
        self.__snapshot_checker = snapshot_checker
        self.__operator_checker = operator_checker

    def get_checker(self, artifact: Artifact):
        """Get the right checker for the given artifact."""
        if isinstance(artifact, VideoArtifact):
            if artifact.recorder == RecorderType.TRAINING:
                return self.__training_checker
            if artifact.recorder == RecorderType.INTERIOR:
                return self.__interior_checker
        if isinstance(artifact, SnapshotArtifact):
            if artifact.recorder == RecorderType.SNAPSHOT:
                return self.__snapshot_checker
        if isinstance(artifact, OperatorArtifact):
            return self.__operator_checker
