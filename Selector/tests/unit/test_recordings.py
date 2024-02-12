""" Selector Tests. """
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, MagicMock
from mongoengine.queryset.visitor import Q
from base.model.artifacts import (RuleOrigin)
from selector.model.recordings import Recordings, RecordingUploadRule, RecordingOptions, SnapshotRecordingEntry,VideoRecordingEntry, RecordingType
from selector.model.recordings_db import DBRecording, DBRecordingRule, DBRecordingOverview


@pytest.mark.unit
class TestRecordings():

    def test_find(self):
        # GIVEN
        device_id = "datanauts"
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(minutes=5)

        mongoengine_query = Mock()
        recording_overview = DBRecordingOverview(tenantID="datanauts",
                                                 deviceID=device_id,
                                                 recording_time=from_ts,
                                                 recording_duration=300.0)

        db_upload_rules = [DBRecordingRule(name="Test Rule",
                                           version="1.0.0",
                                           footage_from=from_ts,
                                           footage_to=to_ts)]
        upload_rules = [RecordingUploadRule(version="1.0.0",
                                            name="Test Rule")]
        rules_dict = {"name": "Test Rule", "version": "1.0.0"}
        db_recording_snap = DBRecording(recording_overview=recording_overview,
                                        video_id="snap_id",
                                        _media_type="image",
                                        upload_rules=db_upload_rules)
        db_recording_video = DBRecording(recording_overview=recording_overview,
                                         video_id="video_id",
                                         _media_type="video",
                                         upload_rules=db_upload_rules)

        snapshot_recording_entry = SnapshotRecordingEntry(device_id=device_id,
                                                          tenant_id=recording_overview.tenantID,
                                                          timestamp=recording_overview.recording_time,
                                                          recording_type=RecordingType.SNAPSHOT,
                                                          upload_rules=upload_rules)
        video_recording_entry = VideoRecordingEntry(device_id=device_id,
                                                    tenant_id=recording_overview.tenantID,
                                                    from_timestamp=from_ts,
                                                    to_timestamp=to_ts,
                                                    duration=recording_overview.recording_duration,
                                                    recording_type=RecordingType.VIDEO,
                                                    upload_rules=upload_rules)
        options = RecordingOptions(
            device_id=device_id,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)
        mongodb_query = Q(
            recording_overview__tenantID="datanauts") & Q(
            recording_overview__deviceID="datanauts") & Q(
            recording_overview__recording_time__gte=from_ts) & Q(
            recording_overview__recording_time__lte=to_ts) & Q(
            upload_rules__match=rules_dict) & mongoengine_query
        DBRecording.objects = Mock(return_value=[db_recording_snap,db_recording_video])
        recordings = Recordings("datanauts")
        # WHEN
        success = [x for x in recordings.find(options)]
        # THEN
        assert len(success) == 2
        assert snapshot_recording_entry in success
        assert video_recording_entry in success
        DBRecording.objects.assert_called_once_with(mongodb_query)

    def test_find_snapshots(self):
        # GIVEN
        device_id = "datanauts"
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(minutes=5)
        mongoengine_query = Mock()
        upload_rules = [RecordingUploadRule(version="1.0.0",
                                            name="Test Rule")]
        snapshot_entry = SnapshotRecordingEntry(device_id=device_id,
                                                tenant_id="datanauts",
                                                timestamp=from_ts,
                                                recording_type=RecordingType.SNAPSHOT,
                                                upload_rules=upload_rules)
        given_options = RecordingOptions(
            device_id=device_id,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        expected_options = RecordingOptions(
            device_id=device_id,
            recording_type=RecordingType.SNAPSHOT,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        recordings = Recordings("datanauts")
        recordings.find = MagicMock()
        recordings.find.return_value = iter([snapshot_entry])

        # WHEN
        success = [x for x in recordings.find_snapshots(given_options)]
        # THEN
        assert snapshot_entry in success
        recordings.find.assert_called_once_with(expected_options)

    def test_find_videos(self):
        # GIVEN
        device_id = "datanauts"
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(minutes=5)
        mongoengine_query = Mock()
        upload_rules = [RecordingUploadRule(version="1.0.0",
                                            name="Test Rule")]
        video_recording_entry = VideoRecordingEntry(device_id=device_id,
                                                    tenant_id="datanauts",
                                                    from_timestamp=from_ts,
                                                    to_timestamp=to_ts,
                                                    duration=5.0,
                                                    recording_type=RecordingType.VIDEO,
                                                    upload_rules=upload_rules)
        given_options = RecordingOptions(
            device_id=device_id,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        expected_options = RecordingOptions(
            device_id=device_id,
            recording_type=RecordingType.VIDEO,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        recordings = Recordings("datanauts")
        recordings.find = MagicMock()
        recordings.find.return_value = iter([video_recording_entry])

        # WHEN
        success = [x for x in recordings.find_videos(given_options)]
        # THEN
        assert video_recording_entry in success
        recordings.find.assert_called_once_with(expected_options)

    def test_count_snapshots(self):
        # GIVEN
        device_id = "datanauts"
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(minutes=5)
        mongoengine_query = Mock()
        upload_rules = [RecordingUploadRule(version="1.0.0",
                                            name="Test Rule")]
        snapshot_entry = SnapshotRecordingEntry(device_id=device_id,
                                                tenant_id="datanauts",
                                                timestamp=from_ts,
                                                recording_type=RecordingType.SNAPSHOT,
                                                upload_rules=upload_rules)
        given_options = RecordingOptions(
            device_id=device_id,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        expected_options = RecordingOptions(
            device_id=device_id,
            recording_type=RecordingType.SNAPSHOT,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        recordings = Recordings("datanauts")
        recordings.count = MagicMock()
        recordings.count.return_value = [snapshot_entry]

        # WHEN
        success = recordings.count_snapshots(given_options)
        # THEN
        assert snapshot_entry in success
        recordings.count.assert_called_once_with(expected_options)

    def test_count_videos(self):
        # GIVEN
        device_id = "datanauts"
        to_ts = datetime.now(timezone.utc)
        from_ts = to_ts - timedelta(minutes=5)
        mongoengine_query = Mock()
        upload_rules = [RecordingUploadRule(version="1.0.0",
                                            name="Test Rule")]
        video_recording_entry = VideoRecordingEntry(device_id=device_id,
                                                    tenant_id="datanauts",
                                                    from_timestamp=from_ts,
                                                    to_timestamp=to_ts,
                                                    duration=5.0,
                                                    recording_type=RecordingType.VIDEO,
                                                    upload_rules=upload_rules)
        given_options = RecordingOptions(
            device_id=device_id,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        expected_options = RecordingOptions(
            device_id=device_id,
            recording_type=RecordingType.VIDEO,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
            upload_rules=upload_rules,
            mongoengine_query=mongoengine_query)

        recordings = Recordings("datanauts")
        recordings.count = MagicMock()
        recordings.count.return_value = [video_recording_entry]

        # WHEN
        success = recordings.count_videos(given_options)
        # THEN
        assert video_recording_entry in success
        recordings.count.assert_called_once_with(expected_options)
