"""Module containing interface for recordings elements in DB."""
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging
from typing import Optional, Iterator
from mongoengine.queryset.visitor import Q, QCombination

from selector.model.recordings_db import DBRecording

_logger = logging.getLogger(__name__)


class RecordingType(Enum):
    """Recording Type Enum"""
    VIDEO = "video"
    SNAPSHOT = "image"


@dataclass
class RecordingUploadRule:
    """Recording Upload Rule that will be used by the algo developers in order to
    make decisions based on past recordings"""
    version: str
    name: str


@dataclass
class RecordingEntry:
    """Recording Entry that will be used by the algo developers in order to make
    decisions based on past recordings"""
    tenant_id: str
    device_id: str
    recording_type: RecordingType
    upload_rules: list[RecordingUploadRule]


@dataclass
class SnapshotRecordingEntry(RecordingEntry):
    """Recording Entry that will be used by the algo developers in order to make
    decisions based on past recordings"""
    timestamp: datetime

    @classmethod
    def load_from_db(cls, db_recording: DBRecording) -> "SnapshotRecordingEntry":
        """Create a SnapshotRecordingEntry from a DBRecording

        Args:
            db_recording (DBRecording): The DBRecording to be converted

        Returns:
            SnapshotRecordingEntry: The SnapshotRecordingEntry created from the DBRecording
        """
        return SnapshotRecordingEntry(
            tenant_id=db_recording.recording_overview.tenantID,
            device_id=db_recording.recording_overview.deviceID,
            recording_type=RecordingType.SNAPSHOT,
            upload_rules=[RecordingUploadRule(
                version=rule.version,
                name=rule.name
            ) for rule in db_recording.upload_rules],
            timestamp=db_recording.recording_overview.recording_time
        )


@dataclass
class VideoRecordingEntry(RecordingEntry):
    """Recording Entry that will be used by the algo developers in order to make
    decisions based on past recordings"""
    from_timestamp: datetime
    to_timestamp: datetime
    duration: float

    @classmethod
    def load_from_db(cls, db_recording: DBRecording) -> "VideoRecordingEntry":
        """Create a VideoRecordingEntry from a DBRecording

        Args:
            db_recording (DBRecording): The DBRecording to be converted

        Returns:
            VideoRecordingEntry: The VideoRecordingEntry created from the DBRecording
        """
        return VideoRecordingEntry(
            tenant_id=db_recording.recording_overview.tenantID,
            device_id=db_recording.recording_overview.deviceID,
            recording_type=RecordingType.VIDEO,
            upload_rules=[
                RecordingUploadRule(
                    version=rule.version,
                    name=rule.name) for rule in db_recording.upload_rules],
            from_timestamp=db_recording.recording_overview.recording_time,
            to_timestamp=db_recording.recording_overview.recording_time +
            timedelta(
                seconds=db_recording.recording_overview.recording_duration),
            duration=db_recording.recording_overview.recording_duration
        )


@dataclass
class RecordingOptions:
    """Options that can be set by the algo developers in order to filter Recordings"""
    device_id: Optional[str] = None
    recording_type: Optional[RecordingType] = None
    from_timestamp: Optional[datetime] = None
    to_timestamp: Optional[datetime] = None
    upload_rules: Optional[list[RecordingUploadRule]] = None
    mongoengine_query: Optional[Q | QCombination] = None


class Recordings:  # pylint: disable=too-many-arguments,protected-access
    """Recordings class that will be used by the algo developers in order to get information
    about past recordings. This class will be used to query the database and get the recordings"""

    def __init__(self, tenant_id: str) -> None:
        self._tenant_id: str = tenant_id

    def __query_generator(self, options: RecordingOptions) -> Q | QCombination:
        """Generates a mongoengine query based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the recordings and generate the query

        Returns:
            Q | QCombination: The mongoengine query generated based on the options
        """
        mongodb_query: Q | QCombination = Q(recording_overview__tenantID=self._tenant_id)
        if options.device_id:
            mongodb_query = mongodb_query & Q(recording_overview__deviceID=options.device_id)
        if options.recording_type:
            mongodb_query = mongodb_query & Q(_media_type=options.recording_type.value)
        if options.from_timestamp:
            mongodb_query = mongodb_query & Q(recording_overview__recording_time__gte=options.from_timestamp)
        if options.to_timestamp:
            mongodb_query = mongodb_query & Q(recording_overview__recording_time__lte=options.to_timestamp)
        if options.upload_rules is not None:
            for rule in options.upload_rules:
                mongodb_rule_query = {
                    "name": rule.name,
                    "version": rule.version
                }
                mongodb_query = mongodb_query & Q(upload_rules__match=mongodb_rule_query)
        if options.mongoengine_query:
            mongodb_query = mongodb_query & options.mongoengine_query

        return mongodb_query

    def find(self, options: RecordingOptions) -> Iterator[RecordingEntry]:
        """ Finds recordings based on the input options parameters

        Args:
            options (RecordingOptions): The options to filter the recordings by

        Yields:
            Iterator[RecordingEntry]: Iterator for recordings found based on the options
        """
        mongodb_query = self.__query_generator(options)

        mongodb_results = DBRecording.objects(mongodb_query)  # pylint: disable=no-member

        for recording in mongodb_results:
            if recording._media_type == RecordingType.SNAPSHOT.value:
                yield SnapshotRecordingEntry.load_from_db(recording)
            elif recording._media_type == RecordingType.VIDEO.value:
                yield VideoRecordingEntry.load_from_db(recording)
            else:
                _logger.warning("Recording type not recognized: %s", recording._media_type)

    def count(self, options: RecordingOptions) -> int:
        """ Counts recordings based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the recordings by

        Returns:
            int: The count of recordings found based on the input parameters
        """
        mongodb_query = self.__query_generator(options)

        return DBRecording.objects(mongodb_query).count()  # pylint: disable=no-member

    def find_snapshots(self, options: RecordingOptions) -> Iterator[RecordingEntry]:
        """ Finds snapshots based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the snapshots by

        Yields:
            list[RecordingEntry]: The list of snapshots found based on the input parameters
        """
        yield from self.find(
            RecordingOptions(
                device_id=options.device_id,
                recording_type=RecordingType.SNAPSHOT,
                from_timestamp=options.from_timestamp,
                to_timestamp=options.to_timestamp,
                upload_rules=options.upload_rules if options.upload_rules is not None else None,
                mongoengine_query=options.mongoengine_query
            )
        )

    def find_videos(self, options: RecordingOptions) -> Iterator[RecordingEntry]:
        """ Finds videos based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the videos by

        Yields:
            Iterator[RecordingEntry]: The list of videos found based on the input parameters
        """
        yield from self.find(
            RecordingOptions(
                device_id=options.device_id,
                recording_type=RecordingType.VIDEO,
                from_timestamp=options.from_timestamp,
                to_timestamp=options.to_timestamp,
                upload_rules=options.upload_rules if options.upload_rules is not None else None,
                mongoengine_query=options.mongoengine_query
            )
        )

    def count_snapshots(self, options: RecordingOptions) -> int:
        """ Counts snapshots based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the snapshots by

        Returns:
            int: The count of snapshots found based on the input parameters
        """
        return self.count(
            RecordingOptions(
                device_id=options.device_id,
                recording_type=RecordingType.SNAPSHOT,
                from_timestamp=options.from_timestamp,
                to_timestamp=options.to_timestamp,
                upload_rules=options.upload_rules if options.upload_rules is not None else None,
                mongoengine_query=options.mongoengine_query
            )
        )

    def count_videos(self, options: RecordingOptions) -> int:
        """ Counts videos based on the input parameters

        Args:
            options (RecordingOptions): The options to filter the videos by

        Returns:
            int: The count of videos found based on the input parameters
        """
        return self.count(
            RecordingOptions(
                device_id=options.device_id,
                recording_type=RecordingType.VIDEO,
                from_timestamp=options.from_timestamp,
                to_timestamp=options.to_timestamp,
                upload_rules=options.upload_rules if options.upload_rules is not None else None,
                mongoengine_query=options.mongoengine_query
            )
        )
