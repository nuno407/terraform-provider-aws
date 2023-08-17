"""Base Rule implementation."""
import logging
from typing import List

from base.model.artifacts import RecorderType
from selector.context import Context
from selector.decision import Decision
from selector.rule import Rule


logger = logging.getLogger(__name__)


class BaseRule(Rule):
    """Base Rule class implementation that extends Rule."""

    def __init__(
        self, attribute_name: str,
        rule_name: str,
    ) -> None:
        """The class constructor.

        Args:
            attribute_name (str): rule attribute name.
            rule_name (str): rule name.
        """
        self._attribute_name = attribute_name
        self._rule_name = rule_name
        super().__init__()

    @property
    def attribute_name(self) -> str:
        """Simple getter for Base Rules

        Returns:
            str: Attribute name to be searched
        """
        return self._attribute_name

    @property
    def rule_name(self) -> str:
        """Getter for rule name

        Returns:
            str: Rule name
        """
        return self._rule_name

    def evaluate(self, context: Context) -> List[Decision]:
        """Evaluate List of Decision

        Args:
            context (Context): context

        Returns:
            List[Decision]: List of Decision tb eval
        """
        return [
            Decision(
                recorder=RecorderType.TRAINING,
                footage_from=context.metadata_artifact.timestamp,
                footage_to=context.metadata_artifact.end_timestamp,
            ),
        ]
