from typing import Dict, List
from setuptools import setup

extras_require: Dict[str, List[str]] = {
    'aws': ['boto3', 'pytz', 'pymongo', 'expiringdict', 'boto3', 'boto3-stubs[kinesis-video-archived-media]', 'boto3-stubs[s3]', 'boto3-stubs[sts]', 'boto3-stubs[kinesisvideo]'], # we should extract pymongo dependency to mongodb package base.mongodb. Also we should put ContainerServices in base package
    'voxel': ['fiftyone', 'fiftyone-teams-app'], # not sure if we can install this here
    'testing': [],
    'mongodb': ['pymongo']
}

setup(
    name='base',
    version='3.0.0',
    packages = ['base'],
    install_requires=[],
    extras_require = {
        "all": [item for dep_list in extras_require.values() for item in dep_list],
        **extras_require
    }
)
