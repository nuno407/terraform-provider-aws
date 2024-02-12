import pytest
from datetime import datetime, timezone
from pymongo import MongoClient
from typing import Optional
from datetime import datetime, timezone
from .helper_functions import load_database
from mongoengine.queryset.visitor import Q
from selector.model.recordings import Recordings, RecordingEntry, RecordingUploadRule, RecordingType, VideoRecordingEntry, SnapshotRecordingEntry, RecordingOptions


@pytest.fixture
def tenant_recordings_interface(
        request,
        mongo_client: MongoClient,
        mongo_recordings_db: str,
        mongo_recordings_collection: str) -> Recordings:
    load_database(mongo_client, mongo_recordings_db, mongo_recordings_collection, "recordings_qa_part_dump.json")
    return Recordings(request.param)


class TestMongoIntegration:
    """
    It uses a local mongo instance.

    THIS TESTS WONT RUN UNLESS A LOCAL MONGO INSTANCE RUNNING.
    There are 2 ways to run a local mongo instance:

    - Docker:
        ```docker run -d -p 27017:27017 mongo```

    - Install/run mongo locally:
        ```sh run_mongo_locally.sh``` (this script is in the root of the project)

    This suite of tests uses a dump from the qa database, the dump is located in the data folder.

    """
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "tenant_recordings_interface,recording_options,result",
        [
            (
                "ridecare_companion_trial",
                RecordingOptions(
                    device_id="rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02",
                    recording_type=RecordingType.VIDEO,
                    from_timestamp=datetime(2024,1,4,20,41,9,986000,tzinfo=timezone.utc),
                    to_timestamp=datetime(2024,3,4,20,41,9,986000,tzinfo=timezone.utc),
                    upload_rules=[RecordingUploadRule(version="1.0.0",name="CHC event every minute")]),
                [
                    VideoRecordingEntry(
                        tenant_id='ridecare_companion_trial',
                        device_id='rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02',
                        recording_type=RecordingType.VIDEO,
                        upload_rules=[
                            RecordingUploadRule(
                                version='1.0.0',
                                name='CHC event every minute'),
                            RecordingUploadRule(
                                version='1.0.0',
                                name='Camera completely blocked')],
                        from_timestamp=datetime(2024,2, 4,20,41,9,986000,tzinfo=timezone.utc),
                        to_timestamp=datetime(2024,2,4,20,52,47,559000,tzinfo=timezone.utc),
                        duration=697.573)
                ]),

            (
                "ridecare_companion_trial",
                RecordingOptions(
                    device_id="rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02",
                    recording_type=RecordingType.SNAPSHOT,
                    from_timestamp=datetime(2024,1,4,20,41,9,986000,tzinfo=timezone.utc),
                    to_timestamp=datetime(2024,2,4,20,7,26,986000,tzinfo=timezone.utc)),
                [
                    SnapshotRecordingEntry(
                        tenant_id='ridecare_companion_trial',
                        device_id='rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02',
                        recording_type=RecordingType.SNAPSHOT,
                        upload_rules=[RecordingUploadRule(version='1.0',name='automatic')],
                        timestamp=datetime(2024,2, 4,20,7,25,283000,tzinfo=timezone.utc))
                ]
            )
        ],
        indirect=["tenant_recordings_interface"])
    def test_find(self,
                  recording_options: RecordingOptions,
                  tenant_recordings_interface: Recordings,
                  result: list[RecordingEntry]) -> None:
        """
        Test the find method of the Recordings interface against a real mongodb.

        Args:
            tenant_recordings_interface (Recordings): _description_
            device_id (Optional[str]): _description_
            recording_type (Optional[RecordingType]): _description_
            from_timestamp (Optional[datetime]): _description_
            to_timestamp (Optional[datetime]): _description_
            upload_rules (list[RecordingUploadRule]): _description_
            mongoengine_query (Optional[Q]): _description_
            result (list[RecordingEntry]): _description_
        """
        real_result = list(
            tenant_recordings_interface.find(recording_options))
        assert real_result == result

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "tenant_recordings_interface,recording_options,result",
        [
            (
                "ridecare_companion_trial",
                RecordingOptions(
                    device_id="rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02",
                    recording_type=RecordingType.VIDEO,
                    from_timestamp=datetime(2024,1,4,20,41,9,986000,tzinfo=timezone.utc),
                    to_timestamp=datetime(2024,3,4,20,41,9,986000,tzinfo=timezone.utc)),
                2
            ),
            (
                "ridecare_companion_trial",
                RecordingOptions(
                    device_id="rc_srx_prod_125627aca97f818127c1af3e168dbd9524b6bd02",
                    recording_type=RecordingType.SNAPSHOT,
                    from_timestamp=datetime(2024,1,4,20,41,9,986000,tzinfo=timezone.utc),
                    to_timestamp=datetime(2024,2,4,20,7,26,986000,tzinfo=timezone.utc)),
                1
            )
        ],
        indirect=["tenant_recordings_interface"])
    def test_count(self,
                   tenant_recordings_interface: Recordings,
                   recording_options: RecordingOptions,
                   result: list[RecordingEntry]) -> None:
        """
        Test the count method of the Recordings interface against a real mongodb.

        Args:
            tenant_recordings_interface (Recordings): _description_
            device_id (Optional[str]): _description_
            recording_type (Optional[RecordingType]): _description_
            from_timestamp (Optional[datetime]): _description_
            to_timestamp (Optional[datetime]): _description_
            upload_rules (list[RecordingUploadRule]): _description_
            mongoengine_query (Optional[Q]): _description_
            result (list[RecordingEntry]): _description_
        """
        real_result = tenant_recordings_interface.count(recording_options)
        assert real_result == result
