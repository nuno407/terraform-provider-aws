""" sdr main module tests """
from unittest.mock import Mock, PropertyMock, patch

import pytest

from sdretriever.main import main  # type: ignore


@pytest.mark.unit
@patch("sdretriever.main.parse_artifact")
def test_main(parse_artifact_mock: Mock):
    graceful_exit = Mock()
    type(graceful_exit).continue_running = PropertyMock(side_effect=[True, False])

    sqs_controller = Mock()
    sqs_controller.get_message = Mock()

    mocked_message = {"Body": """{"Message": { "foo": "bar" }}"""}
    mocked_artifact = "mocked artifact"

    parse_artifact_mock.return_value = mocked_artifact

    sqs_controller.get_message.return_value = mocked_message
    ingestion_handler = Mock()
    ingestion_handler.handle = Mock()

    main(
        graceful_exit=graceful_exit,
        sqs_controller=sqs_controller,
        ingestion_handler=ingestion_handler
    )

    sqs_controller.get_message.assert_called_once()
    parse_artifact_mock.assert_called_once_with({"foo": "bar"})
    ingestion_handler.handle.assert_called_once_with(mocked_artifact, mocked_message)
