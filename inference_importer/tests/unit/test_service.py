""" Test Service Class """
import pytest
import unittest
from unittest.mock import patch, Mock
from datetime import datetime, timezone
from inference_importer.service import InferenceImporter


@pytest.mark.unit
class TestInferenceImporter(unittest.TestCase):

    importer = InferenceImporter()

    def test_create_fn_map(self):
        # Define test data
        output_inferences = [
            "folder_in_s3/sub/inference1.png.out",
            "folder_in_s3/sub/inference2.png.out",
        ]
        source_bucket_name = "source_bucket"
        tmp_dir_name = "/tmp"
        prefix = "folder_in_s3/sub"

        fn_map = self.importer.create_fn_map(output_inferences, source_bucket_name, tmp_dir_name, prefix)

        # Assertions
        self.assertEqual(fn_map, {
            "inference1.png": "s3://source_bucket/inference1.png",
            "inference2.png": "s3://source_bucket/inference2.png"
        })

    @patch('boto3.client')
    def test_initialize_worker(self, mock_boto3_client):
        # Mock the boto3 client
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client

        # Call the method
        self.importer.initialize_worker()

        # Assertions
        mock_boto3_client.assert_called_once_with("s3", region_name="eu-central-1")
        self.assertEqual(self.importer.s3_client, mock_s3_client)
