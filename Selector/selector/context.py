"""Context module"""
from dataclasses import dataclass

from base.model.artifacts import PreviewSignalsArtifact
from selector.model.preview_metadata import PreviewMetadata


@dataclass
class Context:
    """Object containing all values and data sources for a Rule to operate its logic"""
    preview_metadata: PreviewMetadata
    metadata_artifact: PreviewSignalsArtifact
