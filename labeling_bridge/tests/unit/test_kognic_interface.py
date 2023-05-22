"""Test Labelling Bridge Kognic Interface"""
from unittest.mock import ANY, MagicMock, Mock
import kognic.io.model.input as InputModel
import kognic.io.model.input.cameras as CamerasModel

import pytest

from labeling_bridge.kognic_interface import KognicInterface

# pylint: disable=missing-function-docstring


@pytest.fixture(name="kognic_interface")
def fixture_kognic_interface():
    interface = KognicInterface("asd", "123")
    interface.kognic_client = MagicMock()
    return interface


@pytest.mark.unit
@pytest.mark.parametrize("batch1_name,batch2_name,expected",
                         [("dummy_batch_name", "another_batch_name", True),
                          ("wrong_batch_name", "another_batch_name", False)])
def test_verify_batch(batch1_name, batch2_name, expected, kognic_interface: KognicInterface):
    # GIVEN
    batch1 = Mock()
    batch2 = Mock()
    batch1.title = batch1_name
    batch2.title = batch2_name
    kognic_interface.kognic_client.project.get_project_batches = Mock(return_value=[batch1, batch2])
    # WHEN
    value = kognic_interface.verify_batch("dummy_project_id", "dummy_batch_name")
    # THEN
    kognic_interface.kognic_client.project.get_project_batches.assert_called_once_with("dummy_project_id")
    assert value == expected


@pytest.mark.unit
def test_create_batch(kognic_interface: KognicInterface):
    # WHEN
    kognic_interface.create_batch("dummy_project_id", "dummy_batch_name")
    # THEN
    kognic_interface.kognic_client.project.create_batch.assert_called_once_with("dummy_project_id", "dummy_batch_name")


@pytest.mark.unit
def test_upload_image(kognic_interface: KognicInterface):
    # GIVEN
    project_id = "dummy_project_id"
    batch_name = "dummy_batch_name"
    labelling_type = "Splines"
    file_path = "~/some/local/temp/file.jpg"
    file_name = "~/some/file.jpg"
    cameras = CamerasModel.Cameras(
        external_id=file_path,
        frame=CamerasModel.Frame(
            images=[InputModel.Image(filename=file_name, sensor_name="Voxel_export")]
        )
    )
    # WHEN
    kognic_interface.upload_image(project_id, batch_name, labelling_type, file_path, file_name)
    # THEN
    kognic_interface.kognic_client.cameras.create.assert_called_once_with(
        cameras, project=project_id, batch=batch_name, annotation_types=labelling_type, dryrun=ANY)
