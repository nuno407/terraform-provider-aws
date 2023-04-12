"""Installation dependencies for base package."""
from typing import Dict, List

from setuptools import setup

extras_require: Dict[str, List[str]] = {
    # we should extract pymongo dependency to mongodb package base.mongodb.
    # Also we should put ContainerServices in base package
    "aws": [
        "pymongo==3.12",
        "pymongo[srv]==3.12",
        "boto3==1.26.91",
        "pytz==2022.6",
        "pyyaml==6.0",
        "expiringdict==1.2.2",
        "kink==0.6.6",
        "boto3-stubs[kinesis-video-archived-media]==1.26.91",
        "boto3-stubs[s3]==1.26.91",
        "boto3-stubs[sts]==1.26.91",
        "boto3-stubs[kinesisvideo]==1.26.91",
        "boto3-stubs[kinesis-video-archived-media]==1.26.91"
    ],
    "voxel": [
        "fiftyone==0.12.0"
    ],
    # we should use it in the future
    # "monitoring": [
    #     "elastic-apm >= 6.12.0",
    # ],
    "testing": [],
    # for when we separate mongo from ContainerServices
    # "mongodb": [
    #     "pymongo==3.12.3",
    #     "pymongo[srv]==3.12.3",
    # ]
}

setup(
    name="base",
    version="3.0.0",
    packages=["base"],
    install_requires=[],
    extras_require={
        "all": [item for dep_list in extras_require.values() for item in dep_list],
        **extras_require
    }
)
