""" Voxel Base Sample Model """
import logging
from dataclasses import dataclass
from typing import Any, Callable, Union

import fiftyone as fo
import fiftyone.core.media as fom
from fiftyone import ViewField
from fiftyone.core.metadata import ImageMetadata, VideoMetadata

from base.model.artifacts import SnapshotArtifact, VideoArtifact
from base.voxel.functions import (find_or_create_sample,
                                  get_anonymized_path_from_raw, set_field,
                                  set_mandatory_fields_on_sample)
from artifact_api.exceptions import VoxelProcessingException

_logger = logging.getLogger(__name__)


@dataclass
class VoxelFieldContext:
    """Context for each voxel field"""
    sample: fo.Sample
    artifact: Union[SnapshotArtifact, VideoArtifact]
    correlated_anonymized_filepaths: list[str]


class VoxelField:  # pylint: disable=too-few-public-methods
    """Base Class for all voxel field
        In this class we should specify the voxel field type
        and a callable to a callable that receives, as context, the sample and Artifact being processed.
    """
    field_name: str
    field_type: fo.core.fields.Field
    field_subtype: fo.core.fields.Field
    field_value: Callable[[VoxelFieldContext], Any]

    def __init__(self,
                 field_name, field_type: fo.core.fields.Field,
                 field_value: Callable[[VoxelFieldContext], Any],
                 field_subtype: fo.core.fields.Field = None) -> None:
        self.field_name = field_name
        self.field_type = field_type
        self.field_subtype = field_subtype
        self.field_value = field_value


class VoxelSample:  # pylint: disable=too-few-public-methods
    """Base Class for all voxel samples"""

    fields: list[VoxelField]

    @classmethod
    def _compute_sample_metadata(cls, sample: fo.Sample) -> None:
        """
        Calculated the metadata field for the raw and anonymized files.
        Anonymized metadata is stored under "sample.metadata" (voxel default)
        Raw metadata is stored under sample.raw_metadata
        """
        # Calculate metadata for the specified sample
        # (computes metadata for the provided filepath (aka anonymized filepath))
        try:
            # Calculate metadata for the specified sample (computes metadata for the provided filepath)
            sample.compute_metadata()
        except Exception as e:  # pylint: disable=broad-exception-caught, invalid-name
            _logger.warning("Call to compute_metadata() raised an exception: %s", e)

        try:
            # Calculate metadata for the specified sample (computes metadata for the raw file (aka non anonymized))
            if sample.media_type == fom.IMAGE:
                set_field(sample, "raw_metadata", ImageMetadata.build_for(sample.raw_filepath))
            elif sample.media_type == fom.VIDEO:
                set_field(sample, "raw_metadata", VideoMetadata.build_for(sample.raw_filepath))
            else:
                _logger.warning("""raw_metadata not calculated for
                                    sample %s (raw_filepath: %s)""", sample.filepath, sample.raw_filepath)
        except Exception as e:  # pylint: disable=broad-exception-caught, invalid-name
            _logger.warning("Call to build_for() raised an exception: %s", e)

    @classmethod
    def _correlated_paths(cls, correlated_field: str):
        """ Generalizes correlated path merging behavior. """
        def correlates_path_logic(ctx: VoxelFieldContext):
            snapshots_paths: list[str] = ctx.sample.get_field(correlated_field)
            if snapshots_paths is None:
                return list(set(ctx.correlated_anonymized_filepaths))
            snapshots_paths.extend(ctx.correlated_anonymized_filepaths)
            return list(set(snapshots_paths))
        return correlates_path_logic

    @classmethod
    def _update_correlation(cls,
                            correlated: list[str],
                            artifact_filepath: str,
                            dataset_name: str,
                            correlation_field: str) -> None:
        """
        Updates field snapshots_paths and source_videos on a correlated videos
        or snapshot from the newly arrived snapshot or video
        """
        if not fo.dataset_exists(dataset_name):
            return

        dataset = fo.load_dataset(dataset_name)
        correlated_samples = dataset.select_by("filepath", correlated)
        _logger.debug("Correlated samples: %s", str([sample.filepath for sample in correlated_samples]))
        if len(correlated_samples) > 0:
            result = correlated_samples.set_field(
                correlation_field,
                (~ViewField(correlation_field).exists())
                .if_else(
                    [artifact_filepath],
                    ViewField(correlation_field).append(artifact_filepath).unique()
                )
            )
            _logger.debug("Correlated samples source vids field: %s",
                          {sample.filepath: sample[correlation_field] for sample in result})
            result.save()

    @classmethod
    def _verify_dataset_video_fields(cls, dataset: fo.Dataset) -> None:
        """ Function that setups the initial datatypes of the sample fields and his default values. """
        for field in cls.fields:
            if not dataset.has_sample_field(field.field_name):
                if field.field_subtype is not None:
                    dataset.add_sample_field(
                        field_name=field.field_name,
                        ftype=field.field_type,
                        subfield=field.field_subtype
                    )
                else:
                    dataset.add_sample_field(
                        field_name=field.field_name,
                        ftype=field.field_type
                    )
        dataset.save()

    @classmethod
    def _create_sample(cls, artifact: Any, dataset: fo.Dataset, correlated_raw_filepaths: list[str]) -> None:
        """
        Creates or updates a sample
        """
        cls._verify_dataset_video_fields(dataset)
        anon_path = get_anonymized_path_from_raw(artifact.s3_path)

        if anon_path == artifact.s3_path:
            raise VoxelProcessingException(f"Voxel anonymized path is the same as the raw path {anon_path}")

        sample = find_or_create_sample(dataset, anon_path)

        # Seting sample fields
        set_mandatory_fields_on_sample(sample, artifact.tenant_id)

        for field in cls.fields:
            ctx = VoxelFieldContext(sample, artifact, correlated_raw_filepaths)
            result = field.field_value(ctx)
            if result is not None:
                set_field(sample, field.field_name, result)

        cls._compute_sample_metadata(sample)

        _logger.debug("Voxel sample to be saved as: %s", sample.to_dict())
        sample.save()
