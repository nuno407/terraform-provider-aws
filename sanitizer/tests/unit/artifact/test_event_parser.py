import json
import os
from typing import Optional

import pytest

from base.aws.model import MessageAttributes, SQSMessage
from base.model.artifacts import (CameraServiceEventArtifact,
                                  DeviceInfoEventArtifact, EventArtifact,
                                  IncidentEventArtifact, RecorderType,
                                  TimeWindow)
from base.model.event_types import (CameraServiceState, EventType,
                                    GeneralServiceState, IncidentType,
                                    Location, LocationStatus, Shutdown,
                                    ShutdownReason)
from sanitizer.artifact.parsers.event_parser import EventParser
from sanitizer.artifact.parsers.s3_video_parser import S3VideoParser
from sanitizer.exceptions import InvalidMessageError
from helper_functions import load_sqs_json, parse_sqs_message


def _invalid_event_message_body(fixture_file_id: str) -> SQSMessage:
    message = parse_sqs_message(fixture_file_id)
    message.body["Message"]["value"]["properties"]["header"]["message_type"] = "com.bosch.ivs.someUnknown"
    return message


@pytest.mark.unit
@pytest.mark.parametrize("input_message,expected",
                         [(parse_sqs_message("valid_incident_event.json"),
                           [IncidentEventArtifact(tenant_id="datanauts",
                                                  device_id="DATANAUTS_DEV_01",
                                                  timestamp=1690206707606,
                                                  event_name=EventType.INCIDENT,
                                                  incident_type=IncidentType.ACCIDENT_AUTO,
                                                  location=Location(status=LocationStatus.NO_FIX),
                                                  bundle_id="547854021984")]),
                             (parse_sqs_message("valid_camera_service_event.json"),
                              [CameraServiceEventArtifact(tenant_id="jackalope",
                                                          device_id="ivs_srx_develop_min1sf_01",
                                                          timestamp=1690442766559,
                                                          event_name=EventType.CAMERA_SERVICE,
                                                          service_state=GeneralServiceState.AVAILABLE,
                                                          camera_name="InteriorCamera",
                                                          camera_state=[CameraServiceState.CAMERA_READY])]),
                             (parse_sqs_message("valid_device_info_event.json"),
                              [DeviceInfoEventArtifact(tenant_id="jackalope",
                                                       device_id="ivs_srx_develop_min1sf_01",
                                                       timestamp=1690442757402,
                                                       event_name=EventType.DEVICE_INFO,
                                                       system_report="some git infos",
                                                       software_versions=[{"software_name": "BSP",
                                                                           "version": "3.7.0"},
                                                                          {"foo": "bar"}],
                                                       device_type="hailysharey",
                                                       last_shutdown=Shutdown(reason=ShutdownReason.INACTIVITY,
                                                                              reason_description="[SHUTDOWN] No motion detected in ACTIVE. Entering LPM.",
                                                                              timestamp=1690442469352))]),
                             (parse_sqs_message("valid_device_info_event_2.json"),
                              [DeviceInfoEventArtifact(tenant_id="demo_vehiclecare_motionscloud",
                                                       device_id="ivs_slimscaley_prod_268f70e637964d3a5188e0026c17abc113a01539",
                                                       timestamp=1695745201934,
                                                       event_name=EventType.DEVICE_INFO,
                                                       system_report="some git infos",
                                                       software_versions=[{"software_name": "carapplication",
                                                                           "version": "0.0.1"},
                                                                          {"software_name": "damageservice",
                                                                           "version": "3.0.0"},
                                                                          {"software_name": "ivs_algo_smoke",
                                                                           "version": "3.5"},
                                                                          {"software_name": "ivs_car",
                                                                           "version": "1.3.0"},
                                                                          {"software_name": "smokedetectionservice",
                                                                           "version": "1.0.0"}],
                                                       device_type="slimscaley",
                                                       last_shutdown=Shutdown(timestamp=None,
                                                                              shutdown_reason=ShutdownReason.UNKNOWN,
                                                                              shutdown_reason_description=None))])])
def test_event_parser(input_message: SQSMessage,
                      expected: list[EventArtifact]):
    got_events = EventParser().parse(input_message, None)
    assert list(got_events) == expected


@pytest.mark.unit
@pytest.mark.parametrize("input_message,expected_exception", [
    (
        _invalid_event_message_body("valid_device_info_event.json"),
        InvalidMessageError
    )
])
def test_event_parser_fails_as_expected(input_message: SQSMessage,
                                        expected_exception: type):
    with pytest.raises(expected_exception):
        list(EventParser().parse(input_message, None))
