import pytest
from unittest.mock import patch, Mock
from requests import Session, Response
from artifact_downloader.http_client import HttpClient
from artifact_downloader.exceptions import UnexpectedReturnCode


class TestHttpClient:

    @pytest.fixture
    def mock_session(self) -> Session:
        return Mock()

    @pytest.fixture
    def http_client(self, mock_session: Session) -> HttpClient:  # type: ignore
        return HttpClient(mock_session)

    @pytest.mark.unit
    def test_execute_request_success(self, http_client: HttpClient, mock_session: Session):

        # GIVEN
        mock_request = Mock()
        mock_resp = Mock()
        mock_prepare_result = "some_prepare_result"

        mock_resp.status_code = 200
        mock_request.prepare = Mock(return_value=mock_prepare_result)
        mock_session.send = Mock(return_value=mock_resp)  # type: ignore

        # WHEN
        result = http_client.execute_request(mock_request)

        # THEN
        mock_session.send.assert_called_once_with(mock_prepare_result)  # type: ignore
        assert result == mock_resp

    @pytest.mark.unit
    def test_execute_request_error(self, http_client: HttpClient, mock_session: Session):

        # GIVEN
        mock_request = Mock()
        mock_resp = Mock()
        mock_prepare_result = Mock()

        mock_resp.status_code = 404
        mock_request.prepare = Mock(return_value=mock_prepare_result)
        mock_session.send = Mock(return_value=mock_resp)  # type: ignore

        # WHEN
        with pytest.raises(UnexpectedReturnCode):
            http_client.execute_request(mock_request)
