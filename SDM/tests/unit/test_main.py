""" Main test """
import json
from unittest import mock
from unittest.mock import Mock
import pytest
from pytest_mock import MockerFixture
from sdm.main import identify_file, processing_sdm, FileMetadata, main


def _sqs_message_helper(body):
    return {
        "MessageId": "5b3c9451-6151-4134-a8fa-7124c0cc5f4d",
        "ReceiptHandle": "ReceiptHandleKey",
        "MD5OfBody": "117663e84bf3198033abf26ac8fa8c8f",
        "Body": json.dumps(body),
        "Attributes": {
            "SentTimestamp": "1666681944594",
            "ApproximateReceiveCount": "4"
        }
    }


class TestMain():
    """ Main module test. """
    TEST_VIDEO_ID_FIXTURE = "ridecare_companion_trial_rc_srx_prod_5bfe33136f2a43afc3f534f535d402af175914e2_InteriorRecorder_1667387817727_1667388470705"  # pylint: disable=line-too-long

    @pytest.fixture
    def mock_container_services(self, mocker: MockerFixture):
        """ Mock ContainerServices. """
        mock_container = mocker.patch("sdm.main.ContainerServices")
        mock_container.msp_steps = {"TEST_MSP": ["test1", "test2", "CHC"]}
        return mock_container

    @pytest.mark.unit
    @pytest.mark.parametrize("s3_path,expect_metadata", [
        ("mp4", FileMetadata(None, None, None)),
        (TEST_VIDEO_ID_FIXTURE, FileMetadata(None, f"{TEST_VIDEO_ID_FIXTURE}", None)),
        (f"{TEST_VIDEO_ID_FIXTURE}.mp4", FileMetadata(None, f"{TEST_VIDEO_ID_FIXTURE}.mp4", "mp4")),
        (f"Debug_Lync/{TEST_VIDEO_ID_FIXTURE}.mp4", FileMetadata("Debug_Lync", f"{TEST_VIDEO_ID_FIXTURE}.mp4", "mp4"))
    ])
    def test_identify_file(self, s3_path: str, expect_metadata: FileMetadata):
        """ Test processing of the s3 path. """
        # WHEN
        got_metadata = identify_file(s3_path)

        # THEN
        assert got_metadata == expect_metadata, "Method invocation result is not as expected"

    @pytest.mark.unit
    @pytest.mark.parametrize("sqs_message, expected_relay_data", [
        # Test video processing
        (
            _sqs_message_helper({"Service": "Amazon S3",
                                "Event": "s3:ObjectCreated:*",  # This is not checked in code
                                 "Time": "2022-10-25T07:12:24.590Z",
                                 "Bucket": "qa-rcd-raw-video-files",
                                 "RequestId": "44NC5PD94PYDDAV4",
                                 "Records": [
                                         {
                                             "s3": {
                                                 "object": {
                                                     "key": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.mp4"
                                                 }
                                             }
                                         }
                                 ],
                                 "HostId": "super_host"}),
            {
                "processing_steps": ["test1", "test2", "CHC"],
                "s3_path": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.mp4",
                "data_status": "received"
            }
        ),
        # Test snapshot processing
        (
            _sqs_message_helper({"Service": "Amazon S3",
                                "Event": "s3:ObjectCreated:*",  # This is not checked in code
                                 "Time": "2022-10-25T07:12:24.590Z",
                                 "Bucket": "qa-rcd-raw-video-files",
                                 "RequestId": "44NC5PD94PYDDAV4",
                                 "Records": [
                                         {
                                             "s3": {
                                                 "object": {
                                                     "key": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.jpeg"
                                                 }
                                             }
                                         }
                                 ],
                                 "HostId": "super_host"}),
            {
                "processing_steps": ["test1", "test2"],  # We remove CHC entry in case of JPEG/IMAGES
                "s3_path": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.jpeg",
                "data_status": "received"
            }
        ),
        # Test no client in S3 path (ignore)
        (
            _sqs_message_helper({"Service": "Amazon S3",
                                "Event": "s3:ObjectCreated:*",  # This is not checked in code
                                 "Time": "2022-10-25T07:12:24.590Z",
                                 "Bucket": "qa-rcd-raw-video-files",
                                 "RequestId": "44NC5PD94PYDDAV4",
                                 "Records": [
                                         {
                                             "s3": {
                                                 "object": {
                                                     "key": f"{TEST_VIDEO_ID_FIXTURE}.test"
                                                 }
                                             }
                                         }
                                 ],
                                 "HostId": "super_host"}),
            {}
        )
    ])
    def test_processing_sdm(self, mock_container_services: Mock, sqs_message: str, expected_relay_data: dict):
        """ Test SDM. """
        # WHEN
        relay_list = processing_sdm(mock_container_services, Mock(), sqs_message)
        # THEN
        assert relay_list == expected_relay_data

    @pytest.mark.unit
    @pytest.mark.parametrize("sqs_message, expected_relay_data", [
        # Test raw file format processing (ignore)
        (
            _sqs_message_helper({"Service": "Amazon S3",
                                "Event": "s3:ObjectCreated:*",  # This is not checked in code
                                 "Time": "2022-10-25T07:12:24.590Z",
                                 "Bucket": "qa-rcd-raw-video-files",
                                 "RequestId": "44NC5PD94PYDDAV4",
                                 "Records": [
                                         {
                                             "s3": {
                                                 "object": {
                                                     "key": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.raw"
                                                 }
                                             }
                                         }
                                 ],
                                 "HostId": "super_host"}),
            {}
        ),
        # Test random file format processing (ignore)
        (
            _sqs_message_helper({"Service": "Amazon S3",
                                "Event": "s3:ObjectCreated:*",  # This is not checked in code
                                 "Time": "2022-10-25T07:12:24.590Z",
                                 "Bucket": "qa-rcd-raw-video-files",
                                 "RequestId": "44NC5PD94PYDDAV4",
                                 "Records": [
                                         {
                                             "s3": {
                                                 "object": {
                                                     "key": f"TEST_MSP/{TEST_VIDEO_ID_FIXTURE}.test"
                                                 }
                                             }
                                         }
                                 ],
                                 "HostId": "super_host"}),
            {}
        )
    ])
    def test_processing_sdm_exception(self, mock_container_services: Mock, sqs_message: str, expected_relay_data: dict):   # pylint: disable=unused-argument
        """ Test SDM. """
        # WHEN
        with pytest.raises(ValueError):
            processing_sdm(mock_container_services, Mock(), sqs_message)

    @pytest.mark.unit
    @mock.patch("boto3.client")
    @mock.patch("sdm.main.processing_sdm")
    def test_main(self, mock_processing_sdm: Mock, _: Mock, mock_container_services: Mock):
        """ Test Main loop. """
        # GIVEN
        mock_container_services.listen_to_input_queue.return_value = {}
        _stop_condition = Mock(side_effect=[True, False])

        # WHEN
        main(stop_condition=_stop_condition)
        # THEN
        mock_container_services.return_value.listen_to_input_queue.assert_called_once()
        mock_processing_sdm.assert_called_once()
        mock_container_services.return_value.delete_message.assert_called_once()
