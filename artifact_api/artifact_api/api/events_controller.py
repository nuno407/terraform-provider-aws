"""Controller for Events"""
from typing import Union
from fastapi import APIRouter
from fastapi_restful.cbv import cbv
from base.model.artifacts import (SOSOperatorArtifact, PeopleCountOperatorArtifact, CameraBlockedOperatorArtifact,
                                  CameraServiceEventArtifact, DeviceInfoEventArtifact, IncidentEventArtifact)
from artifact_api.models import ResponseMessage

events_router = APIRouter()


@cbv(events_router)
class EventsController:
    """Controller for events"""
    @events_router.post("/ridecare/operator", response_model=ResponseMessage)
    async def process_operator_feedback(self, operator_event: Union[SOSOperatorArtifact,   # pylint: disable=unused-argument
                                                                    PeopleCountOperatorArtifact, CameraBlockedOperatorArtifact]):
        """
        Process the operator event feedback

        Args:
            operator_event (Union[SOSOperatorArtifact,
                        PeopleCountOperatorArtifact, CameraBlockedOperatorArtifact]): _description_
        """
        return {}

    @events_router.post("/ridecare/event", response_model=ResponseMessage)
    async def process_device_event(self, device_event: Union[CameraServiceEventArtifact,    # pylint: disable=unused-argument
                                                             DeviceInfoEventArtifact, IncidentEventArtifact]):
        """
        Process a device event

        Args:
            device_event (Union[CameraServiceEventArtifact,
                        DeviceInfoEventArtifact, IncidentEventArtifact]): _description_
        """
        return {}
