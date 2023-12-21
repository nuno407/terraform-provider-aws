""" Voxel Base Sample Model """
from abc import abstractmethod
from typing import Optional, Any
import logging
from dataclasses import dataclass

import fiftyone as fo
import fiftyone.core.media as fom
from fiftyone import ViewField
from fiftyone.core.metadata import ImageMetadata, VideoMetadata

from base.voxel.functions import (set_field, set_mandatory_fields_on_sample,
                                  find_or_create_sample)

_logger = logging.getLogger(__name__)


@dataclass
class VoxelFieldContext:
    """Context for each voxel field"""
    sample: fo.Sample


class VoxelField:  # pylint: disable=too-few-public-methods
    """Base Class for all voxel field
        In this class we should specify the voxel field type, sub type, if needed, and name
    """
    field_name: str
    field_type: fo.core.fields.Field
    field_subtype: Optional[fo.core.fields.Field]
    field_embedded_type: Optional[fo.core.fields.EmbeddedDocumentField]

    def __init__(self,
                 field_name, field_type: fo.core.fields.Field,
                 field_subtype: fo.core.fields.Field = None,
                 field_embedded_type: fo.core.fields.EmbeddedDocumentField = None) -> None:
        self.field_name = field_name
        self.field_type = field_type
        self.field_subtype = field_subtype
        self.field_embedded_type = field_embedded_type


class VoxelSample:  # pylint: disable=too-few-public-methods
    """Base Class for all voxel samples"""

    @classmethod
    @abstractmethod
    def _get_fields(cls) -> list[VoxelField]:
        """ Should be implemented by subclasses and return all fields for the sample. """

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
    def _append_to_list(cls, list_field: str, values_to_append: list[Any]):
        """ Generalizes merging of list behavior in voxel samples. """
        def extend_path_logic(ctx: VoxelFieldContext):
            current_list_value: list[Any] = ctx.sample.get_field(list_field)
            if current_list_value is None:
                return list(values_to_append)

            if all((isinstance(value, fo.DynamicEmbeddedDocument) for value in values_to_append)):
                current_list_value_in_dict_format = [value.to_json() for value in current_list_value]
                values_to_append_in_dict_format = [value.to_json() for value in values_to_append]
                current_list_value_in_dict_format.extend(values_to_append_in_dict_format)
                unique_values = list(set(current_list_value_in_dict_format))
                return [fo.DynamicEmbeddedDocument.from_json(unique_value) for unique_value in unique_values]

            current_list_value.extend(values_to_append)
            return list(current_list_value)
        return extend_path_logic

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
        for field in cls._get_fields():
            if not dataset.has_sample_field(field.field_name):
                dataset.add_sample_field(
                    field_name=field.field_name,
                    ftype=field.field_type,
                    subfield=field.field_subtype,
                    embedded_doc_type=field.field_embedded_type
                )
        dataset.save()

    @classmethod
    def _upsert_sample(cls, tenant_id: str, dataset: fo.Dataset, anonymized_filepath: str,
                       values_to_set: dict[VoxelField, Any]) -> None:
        """
        Creates or updates a sample
        """
        cls._verify_dataset_video_fields(dataset)
        sample = find_or_create_sample(dataset, anonymized_filepath)

        # Seting sample fields
        set_mandatory_fields_on_sample(sample, tenant_id)

        for voxel_field, value in values_to_set.items():
            if callable(value):
                set_field(sample, voxel_field.field_name, value(VoxelFieldContext(sample=sample)))
            else:
                set_field(sample, voxel_field.field_name, value)

        cls._compute_sample_metadata(sample)

        _logger.debug("Voxel sample to be saved as: %s", sample.to_dict())
        sample.save()
