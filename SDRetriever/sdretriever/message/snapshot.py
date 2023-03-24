"""Snapshot message module """
import logging as log
from datetime import datetime
from typing import Optional

from sdretriever.message.message import Message, Chunk

LOGGER = log.getLogger("SDRetriever." + __name__)

class SnapshotMessage(Message):
    """ Snapshot message class. """

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

    def is_irrelevant(self, tenant_blacklist: Optional[list[str]] = None) -> bool:
        """Runtime tests to determine if message contents are not meant to be ingested

        Returns:
            bool: True if the message is to be deleted without ingestion, otherwise False
        """
        if tenant_blacklist is None:
            tenant_blacklist = []
        try:
            if self.tenant in tenant_blacklist:
                LOGGER.info("Tenant %s is blacklisted messageid=%s", self.tenant, self.messageid)
                return True
        except BaseException: # pylint: disable=broad-exception-caught
            return False
        return False

    @property
    def chunks(self) -> list[Chunk]:
        """ chunks """
        chunks = []
        if "chunk_descriptions" in self.properties:
            chunks = self.properties.get("chunk_descriptions")
            chunks = [Chunk(chunk_description) for chunk_description in chunks]
        else:
            LOGGER.debug("Field 'chunk_descriptions' not found in 'properties'", extra={"messageid": self.messageid})
        return chunks

    @property
    def senttimestamp(self) -> Optional[datetime]:
        """ timestamps """
        senttimestamp = None
        if "SentTimestamp" in self.attributes:
            senttimestamp = self.attributes["SentTimestamp"]
            senttimestamp = datetime.fromtimestamp(int(senttimestamp) / 1000.0)
        else:
            LOGGER.debug("Field 'SentTimestamp' not found in 'Attributes'", extra={"messageid": self.messageid})
        return senttimestamp

    @property
    def eventtype(self) -> str:
        """ event type """
        if "eventType" in self.messageattributes:
            eventtype = self.messageattributes["eventType"]
            # some messages have "Value" - UploadRecordingEvent
            # and others "StringValue" - RecordingEvent
            # Something to check upon, maybe align w/ other teams
            if "Value" in eventtype:
                eventtype = eventtype["Value"]
            elif "StringValue" in eventtype:
                eventtype = eventtype["StringValue"]
            eventtype = eventtype.split(".")[-1]
        else:
            LOGGER.debug("Field 'eventtype' not found in 'MessageAttributes' messageid=%s",
                         self.messageid)
            return ""
        return eventtype

    @property
    def deviceid(self) -> str:
        """ device id """
        if "device_id" in self.header:
            device_id = self.header["device_id"].split(":")[-1]
        else:
            LOGGER.debug("Field 'device_id' not found in 'header' messageid=%s", self.messageid)
            return ""
        return device_id

    @property
    def header(self) -> dict:
        """ header """
        header = {}
        if "header" in self.properties:
            header = self.properties["header"]
        else:
            LOGGER.debug("Field 'header' not found in 'properties' messageid=%s", self.messageid)
        return header

    @property
    def properties(self) -> dict:
        """ properties """
        properties = {}
        if "properties" in self.value:
            properties = self.value["properties"]
        else:
            LOGGER.debug("Field 'properties' not found in 'value' messageid=%s", self.messageid)
        return properties

    @property
    def value(self) -> dict:
        """ value """
        value = {}
        if "value" in self.body:
            value = self.body["value"]
        elif "value" in self.message:
            value = self.message["value"]
        else:
            LOGGER.debug("Field 'value' not found in 'Message' nor 'Body' messageid=%s",
                         self.messageid)
        return value
