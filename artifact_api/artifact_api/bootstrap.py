"""Intilization of dependency injection"""
import os
from kink import di
from base.aws.container_services import ContainerServices
from artifact_api.voxel.voxel_config import VoxelConfig


def bootstrap():
    """Intilize dependency injection"""
    ContainerServices.configure_logging("artifact_api")

    di["tenant_config_path"] = os.getenv("TENANT_MAPPING_CONFIG_PATH", "/app/config/config.yml")
    di["mongodb_config_path"] = os.environ.get("MONGODB_CONFIG", "/app/mongo-conf/mongo_config.yaml")
    di["container_name"] = os.getenv("CONTAINER_NAME", "ArtifactAPI")
    di[VoxelConfig] = VoxelConfig.load_yaml_config(di["tenant_config_path"])
