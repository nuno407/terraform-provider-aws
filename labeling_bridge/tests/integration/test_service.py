"""Test Labelling Bridge service"""
import json
import os
import unittest
from datetime import datetime
from unittest.mock import Mock, patch, ANY
import uuid
import pytest
import fiftyone as fo
from labeling_bridge.repository import Repository
from labeling_bridge.models.api.request_export_job_dto import KognicLabelingTypeDTO
from labeling_bridge.models.database import LabelingJob, LabelingJobTask
from labeling_bridge.models.database.enums import Status
from labeling_bridge.models.api import RequestImportJobDTO
from labeling_bridge.models.api import RequestExportJobDTO

from labeling_bridge.service import ApiService

# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring, too-few-public-methods
# pylint: disable=redefined-outer-name, protected-access

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))


def import_message(labeling_job_name=str(uuid.uuid4())):
    return RequestImportJobDTO(**{
        "kognicProjectId": "dummy_project",
        "labellingJobName": labeling_job_name,
        "clientId": "11111",
        "clientSecret": "aaaaa",
        "dataset": "dummy_dataset"
    })


def create_sample(dataset, filename, tags=None):
    path = f"{__location__}/test_data/{filename}"
    print(path)
    sample = fo.Sample(filepath=path, tags=tags, raw_filepath=path)
    dataset.add_sample(sample, expand_schema=True)
    return sample


def export_message(method, labeling_job_name, labeling_type, tag=None, filters=None, stages=None):
    return RequestExportJobDTO(**{
        "dataset": "dummy_dataset",
        "kognicProjectId": "dummy_project",
        "labellingType": [val.value for val in labeling_type],
        "labellingJobName": labeling_job_name,
        "labellingGuidelines": "dummy_guides",
        "voxelExportMethod": method,
        "clientId": "11111",
        "clientSecret": "aaaaa",
        "voxelTagToExport": tag,
        "filters": filters,
        "stages": stages
    })


@pytest.mark.integration
class TestService:
    @pytest.fixture
    def sample_dataset(self):
        fo.delete_non_persistent_datasets()
        return fo.Dataset(name="dummy_dataset")

    @pytest.fixture
    def api_service(self):
        s3_mock = Mock()
        container_services = Mock()
        return ApiService(s3_mock, container_services)

    @pytest.fixture
    def semseg_test_data(self):
        with (open(os.path.join(__location__, "test_data/test.json"), "r")) as file:
            data_string = file.read()
            data_string = data_string.replace("__location__", __location__)
            data = json.loads(data_string)
            print(data)
        return data

    @pytest.fixture
    def semseg_annotation(self, semseg_test_data):
        semseg = Mock()
        semseg.content = semseg_test_data
        return semseg

    def test_kognic_import_success(self, api_service: ApiService, semseg_annotation, sample_dataset):
        # GIVEN
        labeling_type = [KognicLabelingTypeDTO.SEMSEG]
        kognic_factory = Mock()
        api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
        kognic_factory.get_annotation_types = Mock(return_value=labeling_type)
        kognic_factory.get_project_annotations = Mock(return_value=[semseg_annotation])

        sample_with_labeling_data = create_sample(sample_dataset, "test.png")
        untouched_sample = create_sample(sample_dataset, "test2.png")
        export_msg = export_message("tag", labeling_type=labeling_type, labeling_job_name=str(uuid.uuid4()))
        import_msg = import_message(labeling_job_name=export_msg.labelling_job_name)
        Repository.generate_job_and_task_entries(
            dataset_view=sample_dataset.view(),
            user_email="test@test.com",
            request_export_job_dto=export_msg)

        # WHEN
        print(len(sample_dataset))
        api_service.kognic_import(request_import_job_dto=import_msg)

        # THEN
        assert sample_with_labeling_data.GT_semseg is not None

        today = datetime.combine(datetime.today(), datetime.min.time())
        for detection in sample_with_labeling_data.GT_semseg.detections:
            assert detection.date == today
            assert detection.labeling_job == import_msg.labelling_job_name

        assert untouched_sample.GT_semseg is None
        labeling_job = LabelingJob.objects(kognic_labeling_job_name=export_msg.labelling_job_name).get()
        assert labeling_job.import_export_status.status == Status.DONE
        assert len(LabelingJobTask.objects(kognic_labeling_job=labeling_job)) == 2
        for label_task in LabelingJobTask.objects(kognic_labeling_job=labeling_job):
            assert label_task.import_export_status.status == Status.DONE

    @unittest.mock.patch("fiftyone.core.dataset.Dataset.merge_dir", side_effect=[Exception("Voxel")])
    @unittest.mock.patch("labeling_bridge.service._logger")
    def test_kognic_import_error(self, _logger, _, api_service: ApiService, sample_dataset, semseg_annotation):
        # GIVEN
        labeling_type = [KognicLabelingTypeDTO.SEMSEG]
        kognic_factory = Mock()
        api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
        kognic_factory.get_annotation_types = Mock(return_value=labeling_type)
        kognic_factory.get_project_annotations = Mock(return_value=[semseg_annotation])

        _ = create_sample(sample_dataset, "test.png")
        export_msg = export_message("tag", labeling_type=labeling_type, labeling_job_name=str(uuid.uuid4()))
        import_msg = import_message(labeling_job_name=export_msg.labelling_job_name)
        Repository.generate_job_and_task_entries(
            dataset_view=sample_dataset.view(),
            user_email="test@test.com",
            request_export_job_dto=export_msg)

        # WHEN
        api_service.kognic_import(request_import_job_dto=import_msg)

        # THEN
        _logger.exception.assert_called_once()
        labeling_job = LabelingJob.objects(kognic_labeling_job_name=export_msg.labelling_job_name).get()
        assert labeling_job.import_export_status.status == Status.DONE
        assert len(LabelingJobTask.objects(kognic_labeling_job=labeling_job)) == 1
        for label_task in LabelingJobTask.objects(kognic_labeling_job=labeling_job):
            print(label_task.to_json())
            assert label_task.import_export_status.status == Status.ERROR

    @patch("base.aws.s3.S3Controller.get_s3_path_parts", return_value=["dev-rcd-raw", "path/to/file.png"])
    @patch("base.aws.container_services")
    def test_kognic_export_success(self, container_services, _, api_service: ApiService, sample_dataset):
        # GIVEN
        labeling_type = [KognicLabelingTypeDTO.SEMSEG, KognicLabelingTypeDTO.BODYPOSE]
        kognic_factory = Mock()
        api_service.kognic_interface_factory = Mock(return_value=kognic_factory)
        kognic_factory.verify_batch = Mock(return_value=False)
        kognic_factory.create_batch = Mock()
        kognic_factory.upload_image = Mock()
        kognic_factory.user_email = "test@test.com"
        container_services.download_file_to_disk = Mock()

        exported_sample = create_sample(sample_dataset, "test.png", tags=["export-tag"])
        _additional_sample = create_sample(sample_dataset, "test2.png")

        msg = export_message("tag", labeling_type=labeling_type, labeling_job_name=str(uuid.uuid4()), tag="export-tag")
        # WHEN
        api_service.kognic_export(request_export_job_dto=msg)

        # THEN
        kognic_factory.upload_image.assert_called_once_with(
            "dummy_project", msg.labelling_job_name, [
                val.value for val in labeling_type], exported_sample.filepath, ANY)
        labeling_job = LabelingJob.objects(kognic_labeling_job_name__exact=msg.labelling_job_name).get()
        assert labeling_job.import_export_status.status == Status.PROCESSING
        assert len(LabelingJobTask.objects(kognic_labeling_job=labeling_job)) == 2
        for label_task in LabelingJobTask.objects(kognic_labeling_job=labeling_job):
            assert label_task.import_export_status.status == Status.PROCESSING
            assert label_task.exported_to_kognic_at is not None
