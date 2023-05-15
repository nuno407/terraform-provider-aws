""" healthchek model. """
from dataclasses import dataclass
from typing import NewType


@dataclass
class S3Params():
    """AWS S3 parameters."""
    s3_bucket_anon: str
    s3_bucket_raw: str


DBDocument = NewType("DBDocument", dict)
