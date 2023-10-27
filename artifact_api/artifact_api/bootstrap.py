"""Intilization of dependency injection"""
import os
from kink import di
from base.aws.container_services import ContainerServices
from artifact_api.voxel.voxel_config import VoxelConfig
from artifact_api.config import ArtifactAPIConfig


def bootstrap():
    """Intilize dependency injection"""
    ContainerServices.configure_logging("artifact_api")

    di["config_path"] = os.getenv("ARTIFACT_API_CONFIG_PATH", "/app/config/config.yaml")
    di["tenant_config_path"] = os.getenv("TENANT_MAPPING_CONFIG_PATH", "/app/tenant-mapping-conf/tenant-mapping-conf.yaml")
    di["mongodb_config_path"] = os.environ.get("MONGODB_CONFIG", "/app/mongo-conf/mongo_config.yaml")
    di["container_name"] = os.getenv("CONTAINER_NAME", "ArtifactAPI")
    di[VoxelConfig] = VoxelConfig.load_yaml_config(di["tenant_config_path"])
    di[ArtifactAPIConfig] = ArtifactAPIConfig.load_yaml_config(di["config_path"])
