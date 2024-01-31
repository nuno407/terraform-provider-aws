"""Base Rule implementation."""
import logging
from typing import List

from base.model.artifacts import RecorderType
from selector.model import Context
from selector.decision import Decision
from selector.rule import Rule


logger = logging.getLogger(__name__)


class BaseRule(Rule):
    """Base Rule class implementation that extends Rule."""

    def __init__(
        self, attribute_name: str,
        rule_name: str,
        rule_version: str,
    ) -> None:
        """The class constructor.

        Args:
            attribute_name (str): rule attribute name.
            rule_name (str): rule name.
            rule_version (str): rule version.
        """
        self._attribute_name = attribute_name
        self._rule_name = rule_name
        self._rule_version = rule_version
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

    @property
    def rule_version(self) -> str:
        """Getter for rule version

        Returns:
            str: Rule version
        """
        return self._rule_version

    def evaluate(self, context: Context) -> List[Decision]:
        """Evaluate List of Decision

        Args:
            context (Context): context

        Returns:
            List[Decision]: List of Decision tb eval
        """
        return [
            Decision(
                rule_name=self._rule_name,
                rule_version=self._rule_version,
                recorder=RecorderType.TRAINING,
                footage_from=context.ride_info.start_ride,
                footage_to=context.ride_info.end_ride,
            ),
        ]
