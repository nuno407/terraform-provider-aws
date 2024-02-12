""" A test example for a dummy rule"""
import pytest
from unittest.mock import Mock, ANY
from datetime import datetime, timezone
from ..utils import build_context
from selector.rules.device_ingest_until import DeviceIngestUntil
from selector.model.recordings import RecordingOptions
from selector.decision import Decision
from base.model.artifacts import RecorderType


class TestExampleRule:
    """
    A test example for a rule that uses queries to the recordings collection.

    Each test shall load a real preview metadata file using the build_context function.
    The metadata should be placed inside the test_data in order to be loaded successfully.

    The recordings collection is then accessible in the context object as mock that can be
    asserted according to the test case.
    """
    @ pytest.mark.unit
    def test_rule_device_ingest_until_no_decision(self) -> None:
        """
        Tests the rule DeviceIngestUntil with no decision.
        """

        # GIVEN
        context = build_context("preview_metadata_example_rule.json")

        rule = DeviceIngestUntil()
        context.recordings.count_videos = Mock(return_value=10)  # type: ignore

        # WHEN
        decision = rule.evaluate(context)
        # THEN
        context.recordings.count_videos.assert_called_once_with(
            RecordingOptions(
                device_id='mock_device',
                recording_type=None,
                from_timestamp=ANY,
                to_timestamp=None,
                upload_rules=None,
                mongoengine_query=None))
        assert decision == []

    @ pytest.mark.unit
    def test_rule_device_ingest_until_single_decision(self) -> None:
        """
        Tests the rule DeviceIngestUntil with a single decision.
        """

        # GIVEN
        context = build_context("preview_metadata_example_rule.json")

        rule = DeviceIngestUntil()
        context.recordings.count_videos = Mock(return_value=1)  # type: ignore

        # WHEN
        decision = rule.evaluate(context)

        # THEN
        context.recordings.count_videos.assert_called_once_with(
            RecordingOptions(
                device_id='mock_device',
                recording_type=None,
                from_timestamp=ANY,
                to_timestamp=None,
                upload_rules=None,
                mongoengine_query=None))
        assert decision == [Decision(rule_name='Device ingest until',
                                     rule_version='1.0.0',
                                     recorder=RecorderType.TRAINING,
                                     footage_from=datetime(2023, 7, 3, 8, 38, 29, 461000, tzinfo=timezone.utc),
                                     footage_to=datetime(2023, 7, 3, 8, 48, 29, 461000, tzinfo=timezone.utc))]
