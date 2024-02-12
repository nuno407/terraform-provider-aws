"Evaluator"
import logging
from datetime import timedelta
from typing import Optional

from functional import seq  # type: ignore
from kink import inject

from base.model.artifacts import PreviewSignalsArtifact
from selector.decision import Decision
from selector.model import Context, PreviewMetadata, Recordings, RideInfo
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
        ride_info = RideInfo(
            preview_metadata=preview_metadata,
            start_ride=artifact.timestamp,
            end_ride=artifact.end_timestamp
        )

        # Get recordings interface to add to context
        recordings: Recordings = Recordings(artifact.tenant_id)

        context = Context(
            ride_info=ride_info,
            tenant_id=artifact.tenant_id,
            device_id=artifact.device_id,
            recordings=recordings)
        decisions: list[Decision] = self.__call_rules(context, artifact)
        valid_and_permitted_decisions = (seq(decisions)
                                         .filter(lambda decision: self.__validate_decision(context, decision))
                                         .map(lambda decision: self.__assert_invariants(context, decision))
                                         .filter(lambda decision: decision is not None)
                                         .to_list())

        return valid_and_permitted_decisions

    def __call_rules(self, context: Context, artifact: PreviewSignalsArtifact) -> list[Decision]:
        """Invokes the evaluate() method for every rule in the ruleset

        Args:
            context (Context): object with all information sources that can be used

        Returns:
            list[Decision]: the decisions of every Rule
        """
        decisions: list[Decision] = []

        for rule in self.__ruleset:
            list_of_decisions = rule.evaluate(context)
            decisions.extend(list_of_decisions)
            logger.info(
                "Rule (%s) returned (%d) decisions for tenant (%s), device (%s), \
                recording_id (%s), start_timestamp (%s), end_timestamp (%s)",
                rule.rule_name,
                len(list_of_decisions),
                artifact.tenant_id,
                artifact.device_id,
                artifact.referred_artifact.recording_id,
                artifact.timestamp,
                artifact.end_timestamp)

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
        if decision.footage_from < context.ride_info.start_ride:
            decision.footage_from = context.ride_info.start_ride
        if context.ride_info.end_ride < decision.footage_to:
            decision.footage_to = context.ride_info.end_ride

        # assert the timedelta between consensus_footage_from and
        # consensus_footage_to is less than 2h - MAX allowed by footageAPI
        if decision.footage_to - decision.footage_from > timedelta(hours=2):
            decision.footage_to = decision.footage_from + timedelta(hours=2)

        # check the decision is still valid after the changes
        if decision.footage_to - decision.footage_from < timedelta(seconds=1):
            return None

        return decision
