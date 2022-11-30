
from unittest.mock import Mock

import pytest

from metadata.consumer.service import RelatedMediaService


@ pytest.mark.unit
def test_related_media_service():
    # GIVEN
    db = Mock()
    db.get_video_snapshot_media = Mock(return_value="mock_related")
    related_media = RelatedMediaService(db)

    tenant = "DATANAUTS"
    device = "DATANAUTS_DEV_01"
    start_ms = 1669656796 * 1000
    end_ms = 1669656896 * 1000
    media_type = "image"

    # WHEN
    result = related_media.get_related(tenant, device, start_ms, end_ms, media_type)

    # THEN
    db.get_video_snapshot_media.assert_called_once_with(device, tenant, start_ms, end_ms, media_type)
    assert result == "mock_related"
