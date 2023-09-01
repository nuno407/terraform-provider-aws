"""Tests for SAV Operator event checker"""

from unittest.mock import Mock

from pytest import mark

from healthcheck.checker.sav_operator_events import SAVOperatorArtifactChecker
from healthcheck.database import DBCollection


@mark.unit
def test_sav_operator_checker():
    # GIVEN
    db_controller = Mock()
    checker = SAVOperatorArtifactChecker(db_controller)
    artifact = Mock()

    # WHEN
    checker.run_healthcheck(artifact)

    # THEN
    db_controller.get_db_artifact_or_raise.assert_called_with(artifact, DBCollection.SAV_OPERATOR_FEEDBACK)
