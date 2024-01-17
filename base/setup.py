"""Installation dependencies for base package."""
from typing import Dict, List

from setuptools import setup

extras_require: Dict[str, List[str]] = {
    # we should extract pymongo dependency to mongodb package base.mongodb.
    # Also we should put ContainerServices in base package
    "aws": [
        "pymongo~=4.3",
        "pymongo[srv]~=4.3",
        "pymongo[aws]~=4.3",
        "boto3>= 1.34, < 2.0",
        "mypy_boto3_sqs>= 1.34, < 2.0",
        "mypy_boto3_s3>= 1.34, < 2.0",
        "urllib3<2.1",
        "pytz==2022.6",
        "pyyaml~=6.0",
        "expiringdict~=1.2",
        "aws-error-utils~=2.7",
        "kink==0.6.6"
    ],
    "mongo": [
        "motor~=3.1.2"
    ],
    "model": [
        "pydantic"  # Not forcing version here due to Kognic io in Labeling Service.
    ],
    # we should use it in the future
    # "monitoring": [
    #     "elastic-apm >= 6.12.0",
    # ],
    "voxel": [
        "base[model]",
        "kink==0.6.6",
        "fiftyone==0.15.3",
        # Fiftyone paid version package seems to be missing the install of fiftyone-db
        "fiftyone-db==1.1.0"
    ],
    "testing": [
        "moto[s3]~=4.2.7"
    ],
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
