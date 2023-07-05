"Evaluator"
import logging
from datetime import timedelta
from itertools import chain
from typing import Optional

from functional import seq  # type: ignore
from kink import inject

from base.model.artifacts import PreviewSignalsArtifact
from selector.context import Context
from selector.decision import Decision
from selector.model import PreviewMetadata
from selector.rule import Rule

logger = logging.getLogger(__name__)


@inject
class Evaluator:  # pylint: disable=too-few-public-methods
    """Evaluation process - a map-reduce process that calls all rules to generate a decision,
    used to produce a final consensus"""

    def __init__(self, ruleset: set[Rule]) -> None:
        self.__ruleset: set[Rule] = ruleset

    def evaluate(self, preview_metadata: PreviewMetadata,
                 artifact: PreviewSignalsArtifact) -> list[Decision]:
        """_summary_

        Args:
            preview_metadata (dict): Artifact with payload data uploaded by the device

        Returns:
            permitted_decisions (set(Decision)): all valid outcomes of the evaluation
        """
        logger.info("Evaluating %d rules", len(self.__ruleset))
        context = Context(preview_metadata, artifact)
        decisions: list[Decision] = self.__call_rules(context)
        valid_and_permitted_decisions = (seq(decisions)
                                         .filter(lambda decision: self.__validate_decision(context, decision))
                                         .map(lambda decision: self.__assert_invariants(context, decision))
                                         .filter(lambda decision: decision is not None)
                                         .to_list())
        return valid_and_permitted_decisions

    def __call_rules(self, context: Context) -> list[Decision]:
        """Invokes the evaluate() method for every rule in the ruleset

        Args:
            context (Context): object with all information sources that can be used

        Returns:
            list[Decision]: the decisions of every Rule
        """
        rule_results = list(map(lambda r: r.evaluate(context), self.__ruleset))
        decisions = list(chain.from_iterable(rule_results))
        logger.debug("A total of %d decisions were returned", len(decisions))
        return decisions

    def __validate_decision(self, context: Context, decision: Decision) -> bool:
        """Check ruleset controlled exclusively by the DevCloud developers.
        This method shall ensure rules cannot request data outside of allowed bounds.

        Returns:
            bool: validity of the decision, True if OK, False if not.
        """
        decision_fields = set({decision.recorder, decision.footage_from, decision.footage_to})
        if any(decision_fields) and not all(decision_fields):
            return False
        if not context or not decision:
            return False
        return True

    def __assert_invariants(self, context: Context, decision: Decision) -> Optional[Decision]:
        """Hard limits that cannot be violated in any time.
        These invariants protect the overall system.

        Returns:
            Optional[Decision]: the decision if it is valid or could be corrected, None if not.
        """
        # assert that we don't request more footage than what is allowed in the
        # ride, i.e. footage_from <= consensus_footage_from <=
        # consensus_footage_to <= footage_to
        if decision.footage_from < context.metadata_artifact.timestamp:
            decision.footage_from = context.metadata_artifact.timestamp
        if context.metadata_artifact.end_timestamp < decision.footage_to:
            decision.footage_to = context.metadata_artifact.end_timestamp

        # assert the timedelta between consensus_footage_from and
        # consensus_footage_to is less than 2h - MAX allowed by footageAPI
        if decision.footage_to - decision.footage_from > timedelta(hours=2):
            decision.footage_to = decision.footage_from + timedelta(hours=2)

        # check the decision is still valid after the changes
        if decision.footage_to - decision.footage_from < timedelta(seconds=1):
            return None

        return decision
