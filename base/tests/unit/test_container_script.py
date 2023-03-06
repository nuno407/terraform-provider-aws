""" Test container script. """
import json
from unittest.mock import Mock

import os
import pytest
from base.aws.container_services import ContainerServices


@pytest.mark.usefixtures("s3_client", "rcc_bucket", "rcc_s3_list_prefix")
class TestContainerScripts:  # pylint: disable=missing-function-docstring,missing-class-docstring

    @staticmethod
    def load_json_artifact(artifact_path: str) -> dict:
        with open(f"{os.path.dirname(os.path.abspath(__file__))}/artifacts/{artifact_path}",
                  "r",
                  encoding="utf-8") as file:
            return json.load(file)

    def test_list_s3_objects_more_1000(self, s3_client, rcc_bucket, rcc_s3_list_prefix):
        first_chunk = TestContainerScripts.load_json_artifact("s3_list_5cd8076d1_11_01_16__1.json")
        second_chunk = TestContainerScripts.load_json_artifact("s3_list_5cd8076d1_11_01_16__2.json")
        final_data = TestContainerScripts.load_json_artifact("s3_list_5cd8076d1_11_01_16__concat.json")

        continuation_token = first_chunk["NextContinuationToken"]
        s3_client.list_objects_v2 = Mock(side_effect=[first_chunk, second_chunk])

        response = ContainerServices.list_s3_objects(rcc_s3_list_prefix, rcc_bucket, s3_client, max_iterations=10)

        s3_client.list_objects_v2.assert_any_call(
            Bucket=rcc_bucket,
            Prefix=rcc_s3_list_prefix,
            Delimiter="")
        s3_client.list_objects_v2.assert_called_with(
            Bucket=rcc_bucket,
            ContinuationToken=continuation_token,
            Prefix=rcc_s3_list_prefix,
            Delimiter="")

        assert response == final_data

    def test_list_s3_objects1(self, s3_client, rcc_bucket, rcc_s3_list_prefix):
        first_chunk = TestContainerScripts.load_json_artifact("s3_list_not_truncated.json")
        result_chunk = TestContainerScripts.load_json_artifact("s3_list_not_truncated.json")

        result_chunk["CommonPrefixes"] = []
        s3_client.list_objects_v2 = Mock(return_value=first_chunk)

        response = ContainerServices.list_s3_objects(rcc_s3_list_prefix, rcc_bucket, s3_client, max_iterations=10)

        s3_client.list_objects_v2.assert_called_once_with(
            Bucket=rcc_bucket,
            Prefix=rcc_s3_list_prefix,
            Delimiter="")

        assert response == result_chunk

    def test_list_s3_objects_more_1000_truncated(self, s3_client, rcc_bucket, rcc_s3_list_prefix):
        first_chunk = TestContainerScripts.load_json_artifact("s3_list_5cd8076d1_11_01_16__1.json")
        second_chunk = TestContainerScripts.load_json_artifact("s3_list_5cd8076d1_11_01_16__2.json")
        final_data = first_chunk.copy()

        final_data["CommonPrefixes"] = []
        del final_data["NextContinuationToken"]
        s3_client.list_objects_v2 = Mock(side_effect=[first_chunk, second_chunk])

        response = ContainerServices.list_s3_objects(rcc_s3_list_prefix, rcc_bucket, s3_client)

        s3_client.list_objects_v2.assert_called_once_with(
            Bucket=rcc_bucket,
            Prefix=rcc_s3_list_prefix,
            Delimiter="")

        assert response == final_data

    def test_list_s3_paths(self, s3_client, rcc_bucket, rcc_s3_list_prefix):
        first_chunk = TestContainerScripts.load_json_artifact("s3_list_paths.json")
        final_data = first_chunk.copy()

        final_data["Contents"] = []
        s3_client.list_objects_v2 = Mock(return_value=first_chunk)

        response = ContainerServices.list_s3_objects(rcc_s3_list_prefix, rcc_bucket, s3_client)

        s3_client.list_objects_v2.assert_called_once_with(
            Bucket=rcc_bucket,
            Prefix=rcc_s3_list_prefix,
            Delimiter="")

        assert response == final_data

    def test_load_mongodb_config_vars(self):
        # GIVEN
        os.environ["AWS_CONFIG"] = "tests/unit/test_data/aws_config.yaml"
        os.environ["MONGODB_CONFIG"] = "tests/unit/test_data/mongo_config.yaml"
        container_services = ContainerServices(container="Selector", version="1.0.0")
        # WHEN
        container_services.load_mongodb_config_vars()
        # THEN
        assert container_services.db_tables["algo_output"] == "dev-algorithm-output"
        assert container_services.db_tables["pipeline_exec"] == "dev-pipeline-execution"
        assert container_services.db_tables["recording"] == "dev-recording"
        assert container_services.db_tables["recordings"] == "dev-recordings"
        assert container_services.db_tables["signals"] == "dev-signals"

    def test_load_config_vars(self):
        # GIVEN
        os.environ["AWS_CONFIG"] = "tests/unit/test_data/aws_config.yaml"
        os.environ["MONGODB_CONFIG"] = "tests/unit/test_data/mongo_config.yaml"
        container_services = ContainerServices(container="Selector", version="1.0.0")
        # WHEN
        container_services.load_config_vars()
        # THEN
        assert container_services.sqs_queues_list["SDM"] == "s3-sdm"
        assert container_services.input_queue == "selector"
        assert container_services.msp_steps["Debug_Lync"] == ["Anonymize", "CHC"]
        assert container_services.raw_s3 == "raw_bucket"
        assert container_services.anonymized_s3 == "anon_bucket"
        assert container_services.raw_s3_ignore == ["txt", "json"]
        assert not container_services.anonymized_s3_ignore
        assert container_services.sdr_folder == {}
        assert container_services.rcc_info["role"] == "rcc_role"
        assert container_services.rcc_info["s3_bucket"] == "rcc_bucket"
        assert container_services.ivs_api["address"] == "ivs.api.address"
        assert container_services.secret_managers["selector"] == "footage_api_mdl"
        assert container_services.api_endpoints["mdl_footage_endpoint"] == "https://dummy-endpoint.com/videofootage"
