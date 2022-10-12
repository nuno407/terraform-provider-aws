"""Installation dependencies for base package."""
from typing import Dict
from typing import List

from setuptools import setup

extras_require: Dict[str, List[str]] = {
    # we should extract pymongo dependency to mongodb package base.mongodb.
    # Also we should put ContainerServices in base package
    'aws': [
        'boto3',
        'pytz',
        'pymongo==3.12.3',
        'expiringdict',
        'boto3',
        'boto3-stubs[kinesis-video-archived-media]',
        'boto3-stubs[s3]',
        'boto3-stubs[sts]',
        'boto3-stubs[kinesisvideo]',
        'mypy_boto3_kinesisvideo',
    ],
    # not sure if we can install this here
    'voxel': ['fiftyone==0.9.2', 'fiftyone-teams-app==0.2.2', 'pymongo==3.12.3'],
    'testing': [],
    'mongodb': ['pymongo==3.12.3']
}

setup(
    name='base',
    version='3.0.0',
    packages=['base'],
    install_requires=[],
    extras_require={
        "all": [item for dep_list in extras_require.values() for item in dep_list],
        **extras_require
    }
)
