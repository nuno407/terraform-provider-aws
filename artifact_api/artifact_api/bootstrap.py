"""bootstrap"""

import os
from typing import cast, Union
import yaml
from kink import di
from motor.motor_asyncio import AsyncIOMotorClient
from base.aws.container_services import ContainerServices
from base.mongo.engine import Engine
from base.model.config.policy_config import PolicyConfig
from base.model.artifacts import (SOSOperatorArtifact, CameraBlockedOperatorArtifact,
                                  PeopleCountOperatorArtifact)
from artifact_api.models.mongo_models import (DBCameraServiceEventArtifact,
                                              DBDeviceInfoEventArtifact, DBIncidentEventArtifact, DBIMUSample,
                                              DBS3VideoArtifact, DBSnapshotArtifact, DBPipelineProcessingStatus)
from artifact_api.voxel.voxel_config import VoxelConfig


def create_mongo_client(db_uri: str) -> AsyncIOMotorClient:
    """Function used to easealy mock the mongo client"""
    return AsyncIOMotorClient(db_uri, tz_aware=True)


def bootstrap_di() -> None:
    """Initializes dependency injection autowiring container."""
    ContainerServices.configure_logging("artifact_api")

    di["mongodb_config_path"] = os.environ.get(
        "MONGODB_CONFIG", "/app/mongo-conf/mongo_config.yaml")
    di["db_metadata_tables"] = load_mongodb_config_vars()

    di["tenant_config_path"] = os.getenv(
        "TENANT_MAPPING_CONFIG_PATH",
        "/app/tenant-mapping-conf/tenant-mapping-conf.yaml")
    di["container_name"] = os.getenv("CONTAINER_NAME", "ArtifactAPI")
    di[VoxelConfig] = VoxelConfig.load_yaml_config(di["tenant_config_path"])
    di[PolicyConfig] = di[VoxelConfig].policy_mapping

    db_uri = os.environ["DATABASE_URI"]
    db_name = os.environ["DATABASE_NAME"]
    mongo_client = create_mongo_client(db_uri)

    di["event_engine"] = Engine(model=Union[DBCameraServiceEventArtifact,
                                            DBDeviceInfoEventArtifact,
                                            DBIncidentEventArtifact],
                                database=db_name,
                                collection=di["db_metadata_tables"]["events"],
                                client=mongo_client)
    di["operator_feedback_engine"] = Engine(model=Union[SOSOperatorArtifact,
                                                        PeopleCountOperatorArtifact,
                                                        CameraBlockedOperatorArtifact],
                                            database=db_name,
                                            collection=di["db_metadata_tables"]["sav_operator_feedback"],
                                            client=mongo_client)
    di["snapshot_engine"] = Engine(model=DBSnapshotArtifact,
                                   database=db_name,
                                   collection=di["db_metadata_tables"]["recordings"],
                                   client=mongo_client)
    di["video_engine"] = Engine(model=DBS3VideoArtifact,
                                database=db_name,
                                collection=di["db_metadata_tables"]["recordings"],
                                client=mongo_client)
    di["processed_imu_engine"] = Engine(model=DBIMUSample,
                                        database=db_name,
                                        collection=di["db_metadata_tables"]["processed_imu"],
                                        client=mongo_client)
    di["pipeline_processing_status_engine"] = Engine(model=DBPipelineProcessingStatus,
                                                     database=db_name,
                                                     collection=di["db_metadata_tables"]["pipeline_exec"],
                                                     client=mongo_client)


def load_mongodb_config_vars() -> dict[str, str]:
    """Gets mongodb configuration yaml"""
    with open(di["mongodb_config_path"], "r", encoding="utf-8") as configfile:
        dict_body = dict(yaml.safe_load(configfile).items())

    # Name of the MongoDB tables used to store metadata
    return cast(dict[str, str], dict_body["db_metadata_tables"])
