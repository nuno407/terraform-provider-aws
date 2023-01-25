# type: ignore
# pylint: disable=too-few-public-methods
"""Artifact checker module."""
from typing import Protocol

from healthcheck.model import Artifact


class ArtifactChecker(Protocol):
    """This class acts as an interface and shall have common functions
    between the artifact types to decrease code duplication"""

    def run_healthcheck(self, artifact: Artifact) -> None:
        """Runs the healthcheck for the given artifact"""
