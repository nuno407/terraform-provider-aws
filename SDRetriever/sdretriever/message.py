import json
import logging as log
from abc import abstractmethod
from abc import abstractproperty
from datetime import datetime
from typing import List
from typing import Optional

LOGGER = log.getLogger("SDRetriever." + __name__)


class Chunk(object):
    """Representation of a single message chunk object"""

    def __init__(self, chunk_description: dict = dict()) -> None:
        self.uuid: Optional[str] = chunk_description.get("uuid")
        self.upload_status: Optional[str] = chunk_description.get("upload_status")
        self.start_timestamp_ms: Optional[str] = chunk_description.get("start_timestamp_ms")
        self.end_timestamp_ms: Optional[str] = chunk_description.get("end_timestamp_ms")
        self.payload_size: Optional[str] = chunk_description.get("payload_size")
        self.available: Optional[bool] = self.upload_status == "UPLOAD_STATUS__ALREADY_UPLOADED"

    def __repr__(self) -> str:
        return f"{self.uuid}, [{self.start_timestamp_ms}, {self.end_timestamp_ms}]"


class Message(object):
    """Base class, describes a single, generic SQS message for data ingestion."""

    def __init__(self, sqs_message: dict = None) -> None:
        """Class constructor

        Args:
            sqs_message (dict, optional): Message as read from input queues, without any transformations. Defaults to None.
        """
        if isinstance(sqs_message, dict):
            self.raw_message = sqs_message

    @abstractmethod
    def validate(self):
        """Check for bad format and content of the message.
        If format is bad, send message back to DLQ.
        Last check before working on the message ingestion."""
        pass

    @abstractmethod
    def is_irrelevant(self):
        """Check if a message, independant of its format, is not relevant for us, e.g. regular messages that we ignore e.g. TEST_TENANT.
        If irrelevant, delete, but if relevant, validate afterwards. Must be very strict and only give true positives, let everything else pass.

        # if is_irrelevant:
        #   delete
        # otherwise:
        #   if validate:
        #       process
        #   otherwise:
        #       continue
        """
        pass

    @property
    def messageid(self) -> Optional[str]:
        messageid = None
        if "MessageId" in self.raw_message:
            messageid = self.raw_message.get("MessageId")
        else:
            LOGGER.warning("Field 'MessageId' not found in 'InputMessage'", extra={"messageid": messageid})
        return messageid

    @property
    def receipthandle(self) -> Optional[str]:
        receipthandle = None
        if "ReceiptHandle" in self.raw_message:
            receipthandle = self.raw_message.get("ReceiptHandle")
        else:
            LOGGER.warning("Field 'ReceiptHandle' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return receipthandle

    @property
    def receive_count(self) -> int:
        receive_count = 0
        if 'ApproximateReceiveCount' in self.attributes:
            receive_count = self.attributes['ApproximateReceiveCount']
            receive_count = int(receive_count)
        else:
            LOGGER.warning("Field 'ApproximateReceiveCount' not found in 'Attributes'",
                           extra={"messageid": self.messageid})
        return receive_count

    @property
    def attributes(self) -> dict:
        attributes = {}
        if "Attributes" in self.raw_message:
            attributes = self.raw_message.get("Attributes")
        else:
            LOGGER.warning("Field 'Attributes' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return attributes

    @property
    def body(self) -> dict:
        body = {}
        if "Body" in self.raw_message:
            body = self.raw_message.get("Body")
            if isinstance(body, str):
                body = json.loads(body.replace("\'", "\""))
        else:
            LOGGER.warning("Field 'Body' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return body

    @property
    def message(self) -> dict:
        message = {}
        if "Message" in self.body:
            message = self.body.get("Message")
            if isinstance(message, str):
                message = json.loads(message.replace("\'", "\""))
        else:
            LOGGER.warning("Field 'Message' not found in 'Body'", extra={"messageid": self.messageid})
        return message

    @property
    def messageattributes(self) -> dict:
        messageattributes = {}
        if "MessageAttributes" in self.raw_message:
            messageattributes = self.raw_message.get("MessageAttributes")
            if isinstance(messageattributes, str):
                messageattributes = json.loads(messageattributes.replace("\'", "\""))
        elif "MessageAttributes" in self.body:
            messageattributes = self.body.get("MessageAttributes")
            if isinstance(messageattributes, str):
                messageattributes = json.loads(messageattributes.replace("\'", "\""))
        else:
            LOGGER.warning("Field 'MessageAttributes' not found at root nor in 'Body'",
                           extra={"messageid": self.messageid})
        return messageattributes

    @property
    def timestamp(self) -> Optional[datetime]:
        timestamp = None
        if "timestamp" in self.body:
            timestamp = self.body.get("timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
        elif "Timestamp" in self.body:  # is this the same as self.body["Timestamp"]?
            timestamp = self.body.get("Timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.warning("Field 'timestamp' not found in 'Body' nor 'Message'", extra={"messageid": self.messageid})
        return timestamp

    @property
    def tenant(self) -> Optional[str]:
        tenant = None
        if "tenant" in self.messageattributes:
            tenant = self.messageattributes.get("tenant")
            # some messages have "Value" - UploadRecordingEvent and others "StringValue" - RecordingEvent
            # Something to check upon, maybe align w/ other teams
            if "Value" in tenant:
                tenant = tenant["Value"]
            elif "StringValue" in tenant:
                tenant = tenant["StringValue"]
        else:
            LOGGER.warning("Field 'tenant' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
        return tenant

    @property
    def topicarn(self) -> str:
        """TopicArn is very inconsistent, some messages have it, other have 'topic' instead which has different meaning."""
        topicarn = ""
        if "TopicArn" in self.body:
            topicarn = self.body.get("TopicArn")
            topicarn = topicarn.split(":")[-1]
        else:
            LOGGER.debug("Field 'TopicArn' not found in 'Body'", extra={"messageid": self.messageid})
        return topicarn

    @abstractproperty
    def deviceid(self) -> str:
        pass


class VideoMessage(Message):
    def __init__(self, sqs_message: dict = None) -> None:
        super().__init__(sqs_message)
        self._VIDEO_RECORDING_TYPES = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]

    def video_recording_type(self):
        """Identify the recording type of a particular upload message

        Returns:
            video_recording_type (str): The type of the video event: InteriorRecorder, TrainingRecorder, or FrontRecorder
        """
        video_recording_types = ["InteriorRecorder", "TrainingRecorder", "FrontRecorder"]

        if self.streamname != "":
            type = self.streamname
        elif self.recordingid != "":
            type = self.recordingid

        for video_recording_type in video_recording_types:
            if video_recording_type in type:
                return video_recording_type

    def validate(self) -> bool:
        """Runtime tests to determine if message contents are usable.

        Returns:
            bool: True, False
        """
        # nothing to validate at the moment (tests that we want to send do DLQ when they fail)
        return True

    def is_irrelevant(self, tenant_blacklist: List[str] = [], recorder_blacklist: List[str] = []) -> bool:
        """Runtime tests to determine if message contents are not meant to be ingested. Signals only true positives for irrelevancy.

        Returns:
            bool: True, False
        """
        try:
            if not self.topicarn:
                LOGGER.debug(f"Topic could not be identified", extra={"messageid": self.messageid})
                return True
            if not self.streamname:
                LOGGER.debug("Could not find a stream name", extra={"messageid": self.messageid})
                return True
            if not self.recordingid:
                LOGGER.debug("Could not find a recordingid", extra={"messageid": self.messageid})
                return True
            recorder = self.video_recording_type()
            if recorder in recorder_blacklist:
                LOGGER.info(f"Recorder {recorder} is blacklisted", extra={"messageid": self.messageid})
                return True
            if self.tenant in tenant_blacklist:
                LOGGER.info(f"Tenant {self.tenant} is blacklisted", extra={"messageid": self.messageid})
                return True
            # video messages must have a specific ARN topic
            if not self.topicarn.endswith("video-footage-events"):
                LOGGER.debug(f"Topic '{self.topicarn}' is not for video footage events",
                             extra={"messageid": self.messageid})
                return True
            return False
        except Exception as e:
            LOGGER.warning(
                f"Checks for irrelevancy on VideoMessage raised an exception - {e}",
                extra={
                    "messageid": self.messageid})
            return False

    @property
    def streamname(self) -> Optional[str]:
        streamname = self.message.get("streamName")
        if streamname is None:
            LOGGER.warning("Field 'streamName' not found in 'Message'", extra={"messageid": self.messageid})
        return streamname

    @property
    def recording_type(self) -> Optional[str]:
        if not self.streamname:
            return
        for video_recording_type in self._VIDEO_RECORDING_TYPES:
            if video_recording_type in self.streamname:
                return video_recording_type

    @property
    def recordingid(self) -> str:
        """Messages with topic 'prod-inputEventsTerraform' have a 'recording_id' field,
        but the ones with 'dev-video-footage-events' have a 'recordingId'
        """
        recordingid = ""
        if self.topicarn == 'prod-inputEventsTerraform':
            try:
                recordingid = self.message["value"]["properties"]["recording_id"]
            except BaseException:
                LOGGER.debug("Field 'recording_id' not found in 'properties'", extra={"messageid": self.messageid})

        else:
            if 'recordingId' in self.messageattributes:
                try:
                    recordingid = self.messageattributes["recordingId"]["Value"]
                except BaseException:
                    LOGGER.debug("Field 'recordingId' not found in 'MessageAttributes'",
                                 extra={"messageid": self.messageid})
        return recordingid

    @property
    def footagefrom(self) -> int:
        footagefrom = 0
        if 'footageFrom' in self.message:
            footagefrom = self.message.get("footageFrom")
            # footagefrom = datetime.fromtimestamp(footagefrom/1000.0)#,
            # pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.debug("Field 'footageFrom' not found in 'Message'", extra={"messageid": self.messageid})
        return footagefrom

    @property
    def footageto(self) -> int:
        footageto = 0
        if 'footageTo' in self.message:
            footageto = self.message.get("footageTo")
            # footageto = datetime.fromtimestamp(footageto/1000.0)#,
            # pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.debug("Field 'footageTo' not found in 'Message'", extra={"messageid": self.messageid})
        return footageto

    @property
    def uploadstarted(self) -> Optional[datetime]:
        uploadstarted = None
        if 'uploadStarted' in self.message:
            uploadstarted = self.message["uploadStarted"]
            uploadstarted = datetime.fromtimestamp(uploadstarted / 1000.0)
        else:
            LOGGER.debug("Field 'uploadStarted' not found in 'Message'", extra={"messageid": self.messageid})
        return uploadstarted

    @property
    def uploadfinished(self) -> Optional[datetime]:
        uploadfinished = None
        if 'uploadFinished' in self.message:
            uploadfinished = self.message["uploadFinished"]
            uploadfinished = datetime.fromtimestamp(uploadfinished / 1000.0)
        else:
            LOGGER.debug("Field 'uploadFinished' not found in 'Message'", extra={"messageid": self.messageid})
        return uploadfinished

    @property
    def deviceid(self) -> str:
        if 'deviceId' in self.messageattributes:
            deviceid = self.messageattributes.get("deviceId")['Value']
        else:
            LOGGER.debug("Field 'deviceId' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
            return ''
        return deviceid


class SnapshotMessage(Message):
    def __init__(self, sqs_message: dict = None) -> None:
        super().__init__(sqs_message)

    def validate(self) -> bool:
        """Runtime tests to determine if message contents are usable

        Returns:
            bool: True if valid, else False
        """
        if self.chunks == []:
            LOGGER.debug("Field 'chunk_descriptions' is empty, nothing to ingest",
                         extra={"messageid": self.messageid})
            return False
        return True

    def is_irrelevant(self, tenant_blacklist: List[str] = []) -> bool:
        """Runtime tests to determine if message contents are not meant to be ingested

        Returns:
            bool: True if the message is to be deleted without ingestion, otherwise False
        """
        try:
            if self.tenant in tenant_blacklist:
                LOGGER.info(f"Tenant {self.tenant} is blacklisted", extra={"messageid": self.messageid})
                return True
        except BaseException:
            return False
        return False

    @property
    def chunks(self) -> List[Chunk]:
        chunks = []
        if 'chunk_descriptions' in self.properties:
            chunks = self.properties.get('chunk_descriptions')
            chunks = [Chunk(chunk_description) for chunk_description in chunks]
        else:
            LOGGER.debug("Field 'chunk_descriptions' not found in 'properties'", extra={"messageid": self.messageid})
        return chunks

    @property
    def senttimestamp(self) -> Optional[datetime]:
        senttimestamp = None
        if 'SentTimestamp' in self.attributes:
            senttimestamp = self.attributes['SentTimestamp']
            senttimestamp = datetime.fromtimestamp(int(senttimestamp) / 1000.0)
        else:
            LOGGER.debug("Field 'SentTimestamp' not found in 'Attributes'", extra={"messageid": self.messageid})
        return senttimestamp

    @property
    def eventtype(self) -> str:
        if "eventType" in self.messageattributes:
            eventtype = self.messageattributes["eventType"]
            # some messages have "Value" - UploadRecordingEvent and others "StringValue" - RecordingEvent
            # Something to check upon, maybe align w/ other teams
            if "Value" in eventtype:
                eventtype = eventtype["Value"]
            elif "StringValue" in eventtype:
                eventtype = eventtype["StringValue"]
            eventtype = eventtype.split('.')[-1]
        else:
            LOGGER.debug("Field 'eventtype' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
            return ''
        return eventtype

    @property
    def deviceid(self) -> str:
        if 'device_id' in self.header:
            device_id = self.header["device_id"].split(":")[-1]
        else:
            LOGGER.debug("Field 'device_id' not found in 'header'", extra={"messageid": self.messageid})
            return ''
        return device_id

    @property
    def header(self) -> dict:
        header = {}
        if 'header' in self.properties:
            header = self.properties["header"]
        else:
            LOGGER.debug("Field 'header' not found in 'properties'", extra={"messageid": self.messageid})
        return header

    @property
    def properties(self) -> dict:
        properties = {}
        if 'properties' in self.value:
            properties = self.value["properties"]
        else:
            LOGGER.debug("Field 'properties' not found in 'value'", extra={"messageid": self.messageid})
        return properties

    @property
    def value(self) -> dict:
        value = {}
        if 'value' in self.body:
            value = self.body["value"]
        elif 'value' in self.message:
            value = self.message["value"]
        else:
            LOGGER.debug("Field 'value' not found in 'Message' nor 'Body'", extra={"messageid": self.messageid})
        return value


"""Metadata is not received as message, so we don't have a class to represent it.
Instead, we assume it exists on RCC at the moment we get a notification for its video."""
