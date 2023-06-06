"Selector Rule abstract class"
from abc import ABC, abstractmethod

from selector.context import Context
from selector.decision import Decision


class Rule(ABC):
    """Abstract class for describing some logic rule programmatically"""

    def __init__(self) -> None:
        "No __init()__ needed because a rule is just its operational logic"

    @abstractmethod
    def evaluate(self, context: Context) -> list[Decision]:
        """Applies logic on some Context to determine a Decision

        Args:
            context (Context): all contextual data, including the preview metadata

        Returns:
            list[Decision]: list of outcomes of the evaluation
        """

    @property
    @abstractmethod
    def rule_name(self) -> str:
        """Returns the name for this rule"""
