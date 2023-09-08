""" RENAME_SERVICE test module. """
import pytest
from unittest.mock import ANY, MagicMock, Mock
from inference_importer.service import InferenceImporter
import os
import tempfile
import shutil


def _copy_inference_file_to_temp(file_path, tmp_dir_name, json_file_name):
    destination = os.path.join(tmp_dir_name, json_file_name)
    shutil.copy(file_path, destination)


@pytest.mark.unit
class TestGetInferenceFromS3:

    def test_get_inference_from_s3(self):
        # GIVEN
        file_name = "image_inference_result.png.out"
        json_file_name = file_name.replace(".out", ".json")

        file_path = "./tests/unit/data/" + file_name
        s3_path = "fake_bucket/data/" + file_name
        bucket_name = "fake_bucket"
        job_name = "fake_job"
        importer = InferenceImporter()
        importer.s3_client = Mock()
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dir_name = tmp_dir.name
        importer.s3_client.download_file = Mock(
            side_effect=_copy_inference_file_to_temp(
                file_path, tmp_dir_name, json_file_name))

        # WHEN
        importer.get_inference_from_s3(s3_path, tmp_dir_name, bucket_name, job_name)
        # THEN
        importer.s3_client.download_file.assert_called_once_with(
            bucket_name, s3_path, os.path.join(
                tmp_dir_name, "image_inference_result.png.json"))
        assert os.path.isfile(os.path.join(tmp_dir_name, "image_inference_result.png.json"))
        tmp_dir.cleanup()
