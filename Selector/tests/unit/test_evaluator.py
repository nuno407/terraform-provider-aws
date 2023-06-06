from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from pytz import UTC

from base.model.artifacts import RecorderType
from selector.decision import Decision
from selector.evaluator import Evaluator

dummy_date = datetime(year=2023,month=1,day=10,hour=1,tzinfo=UTC)

from_ts = dummy_date - timedelta(minutes=5)
to_ts = dummy_date


@pytest.mark.unit
class TestEvaluator:
    @pytest.fixture
    def artifact(self):
        artifact = Mock()
        artifact.timestamp = dummy_date - timedelta(minutes=5)
        artifact.end_timestamp = dummy_date
        return artifact

    @pytest.fixture
    def all_day_artifact(self):
        artifact = Mock()
        artifact.timestamp = dummy_date - timedelta(days=1)
        artifact.end_timestamp = dummy_date + timedelta(days=1)
        return artifact

    @pytest.fixture
    def all_rule(self, artifact):
        all_rule = Mock()
        all_rule.evaluate.return_value = [Decision(RecorderType.TRAINING, artifact.timestamp, artifact.end_timestamp)]
        return all_rule

    @pytest.fixture
    def none_rule(self):
        none_rule = Mock()
        none_rule.evaluate.return_value = []
        return none_rule

    @pytest.fixture
    def out_of_ride_bounds_rule(self, artifact):
        from_overlength = artifact.timestamp - timedelta(minutes=5)
        to_overlength = artifact.end_timestamp + timedelta(minutes=5)
        out_of_ride_bounds_rule = Mock()
        out_of_ride_bounds_rule.evaluate.return_value = [
            Decision(RecorderType.TRAINING, from_overlength, to_overlength)]
        return out_of_ride_bounds_rule

    @pytest.fixture
    def too_long_selection_rule(self, artifact):
        from_overlength = dummy_date - timedelta(hours=3)
        to_overlength = dummy_date
        too_long_selection_rule = Mock()
        too_long_selection_rule.evaluate.return_value = [
            Decision(RecorderType.TRAINING, from_overlength, to_overlength)]
        return too_long_selection_rule

    @pytest.fixture
    def defective_rule(self, artifact):
        defective_rule = Mock()
        defective_rule.evaluate.return_value = [
            Decision(RecorderType.TRAINING, artifact.end_timestamp, artifact.timestamp)]
        return defective_rule

    @pytest.fixture
    def metadata(self):
        metadata = Mock()
        return metadata

    def test_all_rule(self, artifact, metadata, all_rule):
        # GIVEN
        evaluator = Evaluator({all_rule})

        # WHEN
        decisions = evaluator.evaluate(metadata, artifact)

        # THEN
        assert len(decisions) == 1
        assert decisions[0].recorder == RecorderType.TRAINING
        assert decisions[0].footage_from == artifact.timestamp
        assert decisions[0].footage_to == artifact.end_timestamp

    def test_none_rule(self, artifact, metadata, none_rule):
        # GIVEN
        evaluator = Evaluator({none_rule})

        # WHEN
        decisions = evaluator.evaluate(metadata, artifact)

        # THEN
        assert len(decisions) == 0

    def test_out_of_ride_bounds_rule(self, artifact, metadata, out_of_ride_bounds_rule):
        # GIVEN
        evaluator = Evaluator({out_of_ride_bounds_rule})

        # WHEN
        decisions = evaluator.evaluate(metadata, artifact)

        # THEN
        assert len(decisions) == 1
        assert decisions[0].recorder == RecorderType.TRAINING
        assert decisions[0].footage_from == artifact.timestamp
        assert decisions[0].footage_to == artifact.end_timestamp

    def test_too_long_selection_rule(self, artifact, metadata, too_long_selection_rule):
        # GIVEN
        evaluator = Evaluator({too_long_selection_rule})

        # WHEN
        decisions = evaluator.evaluate(metadata, artifact)

        # THEN
        assert len(decisions) == 1
        assert decisions[0].recorder == RecorderType.TRAINING
        selected_length = decisions[0].footage_to - decisions[0].footage_from
        assert selected_length - timedelta(hours=2) < timedelta(seconds=1)
        # check truncation happens at the end of the selection
        assert decisions[0].footage_from == too_long_selection_rule.evaluate.return_value[0].footage_from

    def test_defective_rule(self, artifact, metadata, defective_rule):
        # GIVEN
        evaluator = Evaluator({defective_rule})

        # WHEN
        decisions = evaluator.evaluate(metadata, artifact)

        # THEN
        assert len(decisions) == 0
