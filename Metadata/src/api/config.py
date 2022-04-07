import json
import os
import boto3
from api.service import ApiService
from baseaws.shared_functions import ContainerServices
from api.db import Persistence


def get_db_config():
    # Load documentDB login info from config file
    db_config = container_services.docdb_config

    # Create the necessary client for AWS secrets manager access
    secrets_client = boto3.client('secretsmanager',
                                    region_name=db_config['region_name'])

    # Get password and username from secrets manager
    secret_client_response = secrets_client.get_secret_value(SecretId=db_config['secret_name'])
    # Fix the quote notation from Secret client
    secret_json = secret_client_response['SecretString'].replace("\'", "\"")
    # Merge password and username into DB config
    db_config.update(json.loads(secret_json))
    return db_config

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
__db_config = get_db_config()
__db_tables = container_services.db_tables
db = Persistence(__db_config, __db_tables, os.getenv('LOCAL_DEBUG', False))
service = ApiService(db, s3_client)


