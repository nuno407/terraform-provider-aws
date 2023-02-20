"""Metadata service constants."""
import os

AWS_REGION = os.environ.get("AWS_REGION", "eu-central-1")
UNKNOWN_FILE_FORMAT_MESSAGE = "Unknown file format %s"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
