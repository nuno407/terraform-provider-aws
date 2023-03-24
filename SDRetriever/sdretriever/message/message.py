"""Message module. """
import json
import logging as log
from abc import abstractmethod
from datetime import datetime
from typing import Optional

LOGGER = log.getLogger("SDRetriever." + __name__)


class Chunk: # pylint: disable=too-few-public-methods
    """Representation of a single message chunk object"""

    def __init__(self, chunk_description: dict = None) -> None:
        """class init"""
        if chunk_description is None:
            chunk_description = {}
        self.uuid: Optional[str] = chunk_description.get("uuid")
        self.upload_status: Optional[str] = chunk_description.get("upload_status")
        self.start_timestamp_ms: Optional[str] = chunk_description.get("start_timestamp_ms")
        self.end_timestamp_ms: Optional[str] = chunk_description.get("end_timestamp_ms")
        self.payload_size: Optional[str] = chunk_description.get("payload_size")
        self.available: Optional[bool] = self.upload_status == "UPLOAD_STATUS__ALREADY_UPLOADED"

    def __repr__(self) -> str:
        return f"{self.uuid}, [{self.start_timestamp_ms}, {self.end_timestamp_ms}]"


class Message:
    """Base class, describes a single, generic SQS message for data ingestion."""

    def __init__(self, sqs_message: Optional[dict] = None) -> None:
        """Class constructor

        Args:
            sqs_message (dict, optional): Message as read from input queues,
            without any transformations. Defaults to None.
        """
        if isinstance(sqs_message, dict):
            self.raw_message = sqs_message

    @abstractmethod
    def validate(self):
        """Check for bad format and content of the message.
        If format is bad, send message back to DLQ.
        Last check before working on the message ingestion."""

    @abstractmethod
    def is_irrelevant(self):
        """Check if a message, independant of its format, is not relevant for us,
        e.g. regular messages that we ignore e.g. TEST_TENANT.
        If irrelevant, delete, but if relevant, validate afterwards.
        Must be very strict and only give true positives, let everything else pass.

        # if is_irrelevant:
        #   delete
        # otherwise:
        #   if validate:
        #       process
        #   otherwise:
        #       continue
        """

    @property
    def messageid(self) -> Optional[str]:
        """ SQS message id """
        messageid = None
        if "MessageId" in self.raw_message:
            messageid = self.raw_message.get("MessageId")
        else:
            LOGGER.warning("Field 'MessageId' not found in 'InputMessage' messageid=%s", messageid)
        return messageid

    @property
    def receipthandle(self) -> Optional[str]:
        """ SQS message receipt handler """
        receipthandle = None
        if "ReceiptHandle" in self.raw_message:
            receipthandle = self.raw_message.get("ReceiptHandle")
        else:
            LOGGER.warning("Field 'ReceiptHandle' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return receipthandle

    @property
    def receive_count(self) -> int:
        """ SQS message receive count """
        receive_count = 0
        if "ApproximateReceiveCount" in self.attributes:
            receive_count = self.attributes["ApproximateReceiveCount"]
            receive_count = int(receive_count)
        else:
            LOGGER.warning("Field 'ApproximateReceiveCount' not found in 'Attributes'",
                           extra={"messageid": self.messageid})
        return receive_count

    @property
    def attributes(self) -> dict:
        """ SQS message attributes """
        attributes = {}
        if "Attributes" in self.raw_message:
            attributes = self.raw_message.get("Attributes")
        else:
            LOGGER.warning("Field 'Attributes' not found in 'InputMessage'", extra={"messageid": self.messageid})
        return attributes

    @property
    def body(self) -> dict:
        """ SQS message body """
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
        """ inner SQS message body """
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
        """ SQS message attributes """
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
        """ SQS message timestamps """
        timestamp = None
        if "timestamp" in self.body:
            timestamp = self.body.get("timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        elif "Timestamp" in self.body:  # is this the same as self.body["Timestamp"]?
            timestamp = self.body.get("Timestamp")
            timestamp = timestamp[:timestamp.find(".")]
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
            timestamp = datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        else:
            LOGGER.warning("Field 'timestamp' not found in 'Body' \
                            nor 'Message' messageid=%s", self.messageid)
        return timestamp

    @property
    def tenant(self) -> Optional[str]:
        """ SQS message tenant """
        tenant = None
        if "tenant" in self.messageattributes:
            tenant = self.messageattributes.get("tenant")
            # some messages have "Value" - UploadRecordingEvent
            # and others "StringValue" - RecordingEvent
            # Something to check upon, maybe align w/ other teams
            if "Value" in tenant:
                tenant = tenant["Value"]
            elif "StringValue" in tenant:
                tenant = tenant["StringValue"]
        else:
            LOGGER.warning("Field 'tenant' not found in \
                            'MessageAttributes' messageid=%s", self.messageid)
        return tenant

    @property
    def topicarn(self) -> str:
        """TopicArn is very inconsistent, some messages have it,
        other have 'topic' instead which has different meaning."""
        topicarn = ""
        if "TopicArn" in self.body:
            topicarn = self.body.get("TopicArn")
            topicarn = topicarn.split(":")[-1]
        else:
            LOGGER.debug("Field 'TopicArn' not found in 'Body'", extra={"messageid": self.messageid})
        return topicarn

    @property
    @abstractmethod
    def deviceid(self) -> str:
        """ SQS message device id """
