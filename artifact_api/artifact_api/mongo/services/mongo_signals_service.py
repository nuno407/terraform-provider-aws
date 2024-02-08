import logging
from datetime import timedelta
from kink import inject
from base.mongo.engine import Engine
from artifact_api.models.mongo_models import DBSignals
from base.model.validators import LegacyTimeDelta
from base.model.artifacts.api_messages import SignalsFrame

_logger = logging.getLogger(__name__)


@inject
class MongoSignalsService():

    def __init__(self, signals_engine: Engine):
        """
        Constructor

        Args:
            operator_feedback_engine (Engine): operator feedback engine
        """
        self.__signals_engine = signals_engine

    async def save_signals(self, signals: dict[timedelta, SignalsFrame], source: str, recording: str):
        """
        Save signals

        Args:
            signals (dict[timedelta, SignalsFrame]): signals
            source (str): source
            recording (str): recording

        """
        signals_dump: dict[timedelta, dict[str, int | float | bool]] = {k:v.model_dump() for k,v in signals.items()}
        signals_model = DBSignals(source=source, recording=recording, signals=signals_dump)
        await self.__signals_engine.save(signals_model)
        _logger.info("A total of %s signals were saved for source %s and recording %s", len(signals), source, recording)
