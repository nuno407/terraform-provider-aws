import json
import logging as log
from abc import abstractmethod, abstractproperty
from datetime import datetime
from typing import Optional

TENANT_BLACKLIST = {'TEST_TENANT','herbie','jackalope','systestsrx','hacknorris'} # Tenants we receive messages from, but don't have access for 
LOGGER = log.getLogger("SDRetriever")

class Chunk(object):
    """Representation of a single message chunk object"""

    def __init__(self, chunk_description: dict = dict()) -> None:
        self.uuid : str = chunk_description.get("uuid")
        self.upload_status : str = chunk_description.get("upload_status")
        self.start_timestamp_ms : str = chunk_description.get("start_timestamp_ms")
        self.end_timestamp_ms : str = chunk_description.get("end_timestamp_ms")
        self.payload_size : str = chunk_description.get("payload_size")
        self.available : bool = self.upload_status == "UPLOAD_STATUS__ALREADY_UPLOADED" 


    def __repr__(self) -> str:
        return f"{self.uuid}, [{self.start_timestamp_ms}, {self.end_timestamp_ms}]"


class Message(object):
    """Base class, describes a single, generic SQS message for data ingestion."""

    def __init__(self, sqs_message: dict = None) -> None:
        """Class constructor

        Args:
            sqs_message (dict, optional): Message as read from input queues, without any transformations. Defaults to None.
        """
        if type(sqs_message) == dict:
            self.raw_message = sqs_message

    @abstractmethod
    def validate(self):
        pass

    @property
    def messageid(self) -> Optional[str]:
        messageid = None
        if "MessageId" in self.raw_message:
            messageid = self.raw_message.get("MessageId")
        else:
            LOGGER.error("Field 'MessageId' not found in 'InputMessage'", extra={"messageid": messageid})
        return messageid

    @property
    def receipthandle(self) -> Optional[str]:
        receipthandle = None
        if "ReceiptHandle" in self.raw_message:
            receipthandle = self.raw_message.get("ReceiptHandle")
        else:
            LOGGER.error("Field 'ReceiptHandle' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return receipthandle

    @property
    def receive_count(self) -> int:
        receive_count = 0
        if 'ApproximateReceiveCount' in self.attributes:
            receive_count = self.attributes['ApproximateReceiveCount']
            receive_count = int(receive_count)
        else:
            LOGGER.error("Field 'ApproximateReceiveCount' not found in 'Attributes'", extra={"messageid": self.messageid})
        return receive_count

    @property
    def attributes(self) -> dict:
        attributes = {}
        if "Attributes" in self.raw_message:
            attributes = self.raw_message.get("Attributes")
        else:
            LOGGER.error("Field 'Attributes' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return attributes
    @property
    def body(self) -> dict:
        body = {}
        if "Body" in self.raw_message:
            body = self.raw_message.get("Body")
            if type(body) == str:
                body = json.loads(body.replace("\'", "\""))
        else:
            LOGGER.error("Field 'Body' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return body

    @property
    def message(self) -> dict:
        message = {}
        if "Message" in self.body:
            message = self.body.get("Message")
            if type(message) == str:
                message = json.loads(message.replace("\'", "\""))
        else:
            LOGGER.error("Field 'Message' not found in 'Body'", extra={"messageid": self.messageid})
        return message

    @property
    def messageattributes(self) -> dict:
        messageattributes = {}
        if "MessageAttributes" in self.raw_message:
            messageattributes = self.raw_message.get("MessageAttributes")
            if type(messageattributes) == str:
                messageattributes = json.loads(messageattributes.replace("\'", "\""))
        elif "MessageAttributes" in self.body:
            messageattributes = self.body.get("MessageAttributes")
            if type(messageattributes) == str:
                messageattributes = json.loads(messageattributes.replace("\'", "\""))
        else:
            LOGGER.error("Field 'MessageAttributes' not found at root nor in 'Body'", extra={"messageid": self.messageid})
        return messageattributes

    @property
    def timestamp(self) -> Optional[datetime]:
        timestamp = None
        if "timestamp" in self.body:
            timestamp = self.body.get("timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        elif "Timestamp" in self.body:  # is this the same as self.body["Timestamp"]?
            timestamp = self.body.get("Timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.error("Field 'timestamp' not found in 'Body' nor 'Message'", extra={"messageid": self.messageid})
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
            LOGGER.error("Field 'tenant' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
        return tenant

    @property
    def topicarn(self) -> str:
        """TopicArn is very inconsistent, some messages have it, other have 'topic' instead which has different meaning."""
        topicarn = ""
        if "TopicArn" in self.body:
            topicarn = self.body.get("TopicArn")
            topicarn = topicarn.split(":")[-1]
        else:
            LOGGER.error("Field 'TopicArn' not found in 'Body'", extra={"messageid": self.messageid})
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
        """Runtime tests to determine if message contents are usable

        Returns:
            bool: True if valid, else False
        """
        if self.tenant in TENANT_BLACKLIST:
            LOGGER.info(f"Tenant {self.tenant} is blacklisted, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        elif not self.topicarn:
            LOGGER.info(f"Topic could not be identified, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        elif not self.topicarn.endswith("video-footage-events"):
            LOGGER.info(f"Topic '{self.topicarn}' is not for video footage events, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        elif self.streamname == "":
            LOGGER.info("Could not find a stream name, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        # All FrontRecorder data is to be ignored for now
        elif self.video_recording_type() == 'FrontRecorder':
            LOGGER.info("Found 'FrontRecorder' video recorder, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        return True

    @property
    def streamname(self) -> str:
        streamname = ""
        if 'streamName' in self.message:
            streamname = self.message.get("streamName")
        else:
            LOGGER.error("Field 'streamName' not found in 'Message'", extra={"messageid": self.messageid})
        return streamname

    @property
    def recording_type(self) -> str:
        for video_recording_type in self._VIDEO_RECORDING_TYPES:
            if video_recording_type in self.streamname:
                return video_recording_type
        return ""

    @property
    def recordingid(self) -> str:
        """Messages with topic 'prod-inputEventsTerraform' have a 'recording_id' field,
        but the ones with 'dev-video-footage-events' have a 'recordingId'
        """
        recordingid = ""
        if self.topicarn == 'prod-inputEventsTerraform':
            try:
                recordingid = self.message["value"]["properties"]["recording_id"]
            except:
                LOGGER.error("Field 'recording_id' not found in 'properties'", extra={"messageid": self.messageid})

        else:
            if 'recordingId' in self.messageattributes:
                try:
                    recordingid = self.messageattributes["recordingId"]["Value"]
                except:
                    LOGGER.error("Field 'recordingId' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
        return recordingid

    @property
    def footagefrom(self) -> int:
        footagefrom = 0
        if 'footageFrom' in self.message:
            footagefrom = self.message.get("footageFrom")
            #footagefrom = datetime.fromtimestamp(footagefrom/1000.0)#, pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.error("Field 'footageFrom' not found in 'Message'", extra={"messageid": self.messageid})
        return footagefrom

    @property
    def footageto(self) -> int:
        footageto = 0
        if 'footageTo' in self.message:
            footageto = self.message.get("footageTo")
            #footageto = datetime.fromtimestamp(footageto/1000.0)#, pytz.timezone('Europe/Berlin'))#.strftime('%Y-%m-%d %H:%M:%S')
        else:
            LOGGER.error("Field 'footageTo' not found in 'Message'", extra={"messageid": self.messageid})
        return footageto

    @property
    def uploadstarted(self) -> Optional[datetime]:
        uploadstarted = None
        if 'uploadStarted' in self.message:
            uploadstarted = self.message["uploadStarted"]
            uploadstarted = datetime.fromtimestamp(uploadstarted/1000.0)
        else:
            LOGGER.error("Field 'uploadStarted' not found in 'Message'", extra={"messageid": self.messageid})
        return uploadstarted

    @property
    def uploadfinished(self) -> Optional[datetime]:
        uploadfinished = None
        if 'uploadFinished' in self.message:
            uploadfinished = self.message["uploadFinished"]
            uploadfinished = datetime.fromtimestamp(uploadfinished/1000.0)
        else:
            LOGGER.error("Field 'uploadFinished' not found in 'Message'", extra={"messageid": self.messageid})
        return uploadfinished

    @property
    def deviceid(self) -> str:
        if 'deviceId' in self.messageattributes:
            deviceid = self.messageattributes.get("deviceId")['Value']
        else:
            LOGGER.error("Field 'deviceId' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
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
        if self.tenant in TENANT_BLACKLIST:
            LOGGER.info(f"Tenant {self.tenant} is blacklisted, message deemed invalid for ingestion", extra={"messageid": self.messageid})
            return False
        return True


    @property
    def chunks(self) -> list[Chunk]:
        chunks = []
        if 'chunk_descriptions' in self.properties:
            chunks = self.properties.get('chunk_descriptions')
            chunks = [Chunk(chunk_description) for chunk_description in chunks]
            if chunks == []:
                LOGGER.warning("Field 'chunk_descriptions' is empty", extra={"messageid": self.messageid})
        else:
            LOGGER.error("Field 'chunk_descriptions' not found in 'properties'", extra={"messageid": self.messageid})
        return chunks

    @property
    def senttimestamp(self) -> Optional[datetime]:
        senttimestamp = None
        if 'SentTimestamp' in self.attributes:
            senttimestamp = self.attributes['SentTimestamp']
            senttimestamp = datetime.fromtimestamp(int(senttimestamp)/1000.0)
        else:
            LOGGER.error("Field 'SentTimestamp' not found in 'Attributes'", extra={"messageid": self.messageid})
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
            LOGGER.error("Field 'eventtype' not found in 'MessageAttributes'", extra={"messageid": self.messageid})
            return ''
        return eventtype

    @property
    def deviceid(self) -> str:
        if 'device_id' in self.header:
            device_id = self.header["device_id"].split(":")[-1]
        else:
            LOGGER.error("Field 'device_id' not found in 'header'", extra={"messageid": self.messageid})
            return ''
        return device_id

    @property
    def header(self) -> dict:
        header = {}
        if 'header' in self.properties:
            header = self.properties["header"]
        else:
            LOGGER.error("Field 'header' not found in 'properties'", extra={"messageid": self.messageid})
        return header

    @property
    def properties(self) -> dict:
        properties = {}
        if 'properties' in self.value:
            properties = self.value["properties"]
        else:
            LOGGER.error("Field 'properties' not found in 'value'", extra={"messageid": self.messageid})
        return properties

    @property
    def value(self) -> dict:
        value = {}
        if 'value' in self.body:
            value = self.body["value"]
        elif 'value' in self.message:
            value = self.message["value"]
        else:
            LOGGER.error("Field 'value' not found in 'Message' nor 'Body'", extra={"messageid": self.messageid})
        return value

"""Metadata is not received as message, so we don't have a class to represent it.
Instead, we assume it exists on RCC at the moment we get a notification for its video."""
