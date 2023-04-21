import fiftyone as fo
import logging
from Metadata.metadata.consumer.voxel.metadata_parser import VoxelFrameParser
from metadata.consumer.voxel.constants import CLASSIFICATION_LABEL, POSE_LABEL
_logger = logging.getLogger(__name__)


class VoxelMetadataLoader:

    @staticmethod
    def load_snapshot_metadata(sample: fo.Sample, snapshot_metadata: dict):
        """
        Given a sample from voxel and the metadata from the snapshot, loads the poses and bounding boxes into the sample.
        The caller is responsible for calling the "save" method into the voxel dataset.

        Currently it only loads "floatAttributes", pose keypoints and bounding boxes.
        Args:
            sample (fo.Sample): Voxel sample.
            snapshot_metadata (dict): The metadata of the snapshot already parsed.
        """
        snapshot_width = int(snapshot_metadata["resolution"]["width"])
        snapshot_height = int(snapshot_metadata["resolution"]["height"])

        frame_processor = VoxelFrameParser(snapshot_width, snapshot_height)

        if "frame" not in snapshot_metadata:
            _logger.info("The current snapshot doesn't have metadata. Skipping voxel metadata update.")
            return

        list_frames = snapshot_metadata["frame"]

        if len(list_frames) == 0:
            _logger.info("The current snapshot doesn't have metadata. Skipping voxel metadata update.")
            return
        elif len(list_frames) >= 1:
            raise ValueError("The metadata of the snapshot must have just one frame")

        frame_processor.parse(snapshot_metadata["frame"][0])
        sample[POSE_LABEL] = frame_processor.get_keypoints()
        # sample[BBOX_LABEL] = frame_processor.get_bouding_boxes()
        sample[CLASSIFICATION_LABEL] = frame_processor.get_classifications()

        # Log
        _logger.debug(
            "Snapshot's metadata with %sx%d resolution has been added to the sample",
            snapshot_width,
            snapshot_height)
