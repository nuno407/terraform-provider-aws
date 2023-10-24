import fiftyone as fo
import logging
from kink import inject
from artifact_api.voxel.voxel_config import VoxelConfig

_logger = logging.getLogger(__name__)


@inject
class VoxelService:
    """
    Class responsible for managing the create and update samples
    """

    def __init__(self, voxel_config: VoxelConfig):
        self.__voxel_config = voxel_config
