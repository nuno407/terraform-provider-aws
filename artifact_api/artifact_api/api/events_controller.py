"""Controller for Events"""
from typing import Union
from fastapi import APIRouter, Depends
from fastapi_restful.cbv import cbv
from kink import di
from base.model.artifacts import (CameraServiceEventArtifact, CameraBlockedOperatorArtifact, DeviceInfoEventArtifact,
                                  IncidentEventArtifact, PeopleCountOperatorArtifact, SOSOperatorArtifact)
from artifact_api.mongo.mongo_service import MongoService
from artifact_api.models import ResponseMessage


events_router = APIRouter()


@cbv(events_router)
class EventsController:
    """Controller for events"""

    @events_router.post("/ridecare/event", response_model=ResponseMessage)
    async def process_device_event(self, message: Union[CameraServiceEventArtifact,
                                                        DeviceInfoEventArtifact, IncidentEventArtifact],
                                   mongo_service: MongoService = Depends(lambda: di[MongoService])):
        """
        Process a device event

        Args:
            device_event (Union[CameraServiceEventArtifact,
                        DeviceInfoEventArtifact, IncidentEventArtifact]): _description_
        """
        await mongo_service.create_event(message=message)
        return ResponseMessage()

    @events_router.post("/ridecare/operator", response_model=ResponseMessage)
    async def process_operator_feedback(self, operator_feedback_event: Union[SOSOperatorArtifact,
                                                                             PeopleCountOperatorArtifact,
                                                                             CameraBlockedOperatorArtifact],
                                        mongo_service: MongoService = Depends(lambda: di[MongoService])):
        """
        Process the operator feedback event

        Args:
            operator_feedback_event (Union[SOSOperatorArtifact,
                            PeopleCountOperatorArtifact, CameraBlockedOperatorArtifact]): Operator feedback artifact
        """
        await mongo_service.create_operator_feedback_event(operator_feedback_event)
        return ResponseMessage()
