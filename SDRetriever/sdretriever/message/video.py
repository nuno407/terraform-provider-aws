""" Message module. """
import logging as log
from datetime import datetime
from typing import Optional

from sdretriever.message.message import Message

LOGGER = log.getLogger("SDRetriever." + __name__)


class VideoMessage(Message):
    """ Video message """

    def __init__(self, sqs_message: dict = None) -> None:
        super().__init__(sqs_message)
        self.__video_recording_types = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]

    def video_recording_type(self):
        """Identify the recording type of a particular upload message

        Returns:
            video_recording_type (str): The type of the video event:
            InteriorRecorder, TrainingRecorder, or FrontRecorder
        """
        video_recording_types = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]

        if self.streamname != "":
            message_type = self.streamname
        elif self.recordingid != "":
            message_type = self.recordingid

        for video_recording_type in video_recording_types:
            if video_recording_type in message_type:
                return video_recording_type
        return None

    def validate(self) -> bool:
        """Runtime tests to determine if message contents are usable.

        Returns:
            bool: True, False
        """
        # nothing to validate at the moment (tests that we want to send do DLQ when they fail)
        return True

    def __check_blacklists(self, tenant_blacklist, recorder_blacklist):
        recorder = self.video_recording_type()
        if recorder in recorder_blacklist:
            LOGGER.info("Recorder %s is blacklisted messageid=%s", recorder, self.messageid)
            return True
        if self.tenant in tenant_blacklist:
            LOGGER.info("Tenant %s is blacklisted messageid=%s", self.tenant, self.messageid)
            return True
        # video messages must have a specific ARN topic
        if not self.topicarn.endswith("video-footage-events"):
            LOGGER.debug("Topic %s is not for video footage events messageid=%s",
                         self.topicarn, self.messageid)
            return True
        return False

    def __check_conditions(self):
        """ Checks recorder conditions for irrelevant """
        if not self.topicarn:
            LOGGER.debug("Topic could not be identified messageid=%s", self.messageid)
            return True
        if not self.streamname:
            LOGGER.debug("Could not find a stream name messageid=%s", self.messageid)
            return True
        if not self.recordingid:
            LOGGER.debug("Could not find a recordingid messageid=%s", self.messageid)
            return True
        return False

    def is_irrelevant(self, tenant_blacklist: Optional[list[str]] = None,
                      recorder_blacklist: Optional[list[str]] = None) -> bool:
        """Runtime tests to determine if message contents are not meant to be ingested.
        Signals only true positives for irrelevancy.

        Returns:
            bool: True, False
        """
        if tenant_blacklist is None:
            tenant_blacklist = []
        if recorder_blacklist is None:
            recorder_blacklist = []
        try:
            return self.__check_blacklists(tenant_blacklist, recorder_blacklist) or self.__check_conditions()
        except Exception as err:  # pylint: disable=broad-exception-caught
            LOGGER.warning("Checks for irrelevancy on VideoMessage raised an exception -\
                            %s messageid=%s",
                           err,
                           self.messageid)
            return False

    @property
    def streamname(self) -> Optional[str]:
        """ The name of the stream """
        streamname = self.message.get("streamName")
        if streamname is None:
            LOGGER.warning("Field 'streamName' not found in 'Message' messageid=%s", self.messageid)
        return streamname

    @property
    def recording_type(self) -> Optional[str]:  # pylint: disable=inconsistent-return-statements
        """ the recording type """
        if not self.streamname:
            return None
        for video_recording_type in self.__video_recording_types:
            if video_recording_type in self.streamname:
                return video_recording_type

    @property
    def recordingid(self) -> str:
        """Messages with topic 'prod-inputEventsTerraform' have a 'recording_id' field,
        but the ones with 'dev-video-footage-events' have a 'recordingId'
        """
        recordingid = ""
        if self.topicarn == "prod-inputEventsTerraform":
            try:
                recordingid = self.message["value"]["properties"]["recording_id"]
            except KeyError:
                LOGGER.debug("Field 'recording_id' not found in 'properties' messageid=%s",
                             self.messageid)

        else:
            if "recordingId" in self.messageattributes:
                try:
                    recordingid = self.messageattributes["recordingId"]["Value"]
                except KeyError:
                    LOGGER.debug("Field 'Value' not found in 'recordingId' messageid=%s",
                                 self.messageid)
        return recordingid

    @property
    def footagefrom(self) -> int:
        """ footageFrom """
        footagefrom = 0
        if "footageFrom" in self.message:
            footagefrom = self.message.get("footageFrom")
            # footagefrom = datetime.fromtimestamp(footagefrom/1000.0)#,
            # pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.debug("Field 'footageFrom' not found in 'Message' messageid=%s", self.messageid)
        return footagefrom

    @property
    def footageto(self) -> int:
        """ footageTo """
        footageto = 0
        if "footageTo" in self.message:
            footageto = self.message.get("footageTo")
            # footageto = datetime.fromtimestamp(footageto/1000.0)#,
            # pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.debug("Field 'footageTo' not found in 'Message' messageid=%s", self.messageid)
        return footageto

    @property
    def uploadstarted(self) -> Optional[datetime]:
        """ upload started """
        uploadstarted = None
        if "uploadStarted" in self.message:
            uploadstarted = self.message["uploadStarted"]
            uploadstarted = datetime.fromtimestamp(uploadstarted / 1000.0)
        else:
            LOGGER.debug("Field 'uploadStarted' not found in 'Message' messageid=%s",
                         self.messageid)
        return uploadstarted

    @property
    def uploadfinished(self) -> Optional[datetime]:
        """ upload finished """
        uploadfinished = None
        if "uploadFinished" in self.message:
            uploadfinished = self.message["uploadFinished"]
            uploadfinished = datetime.fromtimestamp(uploadfinished / 1000.0)
        else:
            LOGGER.debug("Field 'uploadFinished' not found in 'Message' messageid=%s",
                         self.messageid)
        return uploadfinished

    @property
    def deviceid(self) -> str:
        """ device id """
        if "deviceId" in self.messageattributes:
            deviceid = self.messageattributes.get("deviceId")["Value"]
        else:
            LOGGER.debug("Field 'deviceId' not found in 'MessageAttributes' messageid=%s",
                         self.messageid)
            return ""
        return deviceid
