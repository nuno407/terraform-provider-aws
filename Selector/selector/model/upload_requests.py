"""Class for database document for Upload Requests"""
import logging
from mongoengine import Document, StringField, DateTimeField

_logger = logging.getLogger(__name__)


class DBDecision(Document):
    """Database document for Upload Requests"""
    rule_name = StringField(required=True)
    rule_version = StringField(required=True)
    origin = StringField(required=True)
    tenant = StringField(required=True)
    footage_id = StringField(required=True)
    footage_from = DateTimeField(required=True)
    footage_to = DateTimeField(required=True)

    meta = {
        "collection": "upload-requests",
        "allow_inheritance": True,
        "db_alias": "SelectorDB"
    }

    def save_db_decision(self):
        """Saves a DBDecision and logs it"""
        self.save()
        _logger.info("Request returned footage_id %s, and decision saved in db for rule %s",
                     self.footage_id, self.rule_name)
