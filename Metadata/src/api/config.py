import json
import os
import boto3
from api.service import ApiService
from baseaws.shared_functions import ContainerServices
from api.db import Persistence

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v8.0"

# Create the necessary clients for AWS services access
s3_client = boto3.client('s3',
                            region_name='eu-central-1')

# Initialise instance of ContainerServices class
container_services = ContainerServices(container=CONTAINER_NAME,
                                        version=CONTAINER_VERSION)

# Load global variable values from config json file (S3 bucket)
container_services.load_config_vars(s3_client)

# Mongo client creation with info previously built
__db_connstring = container_services.get_db_connstring()
__db_tables = container_services.db_tables
db = Persistence(__db_connstring, __db_tables)
service = ApiService(db, s3_client)


