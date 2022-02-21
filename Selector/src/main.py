"""Selector container script"""
import logging
import boto3
from selector import Selector
from baseaws.shared_functions import ContainerServices


CONTAINER_NAME = "Selector"    # Name of the current container
CONTAINER_VERSION = "v1.0"      # Version of the current container                     

def main():
    """Main function"""

    # Define configuration for logging messages
    logging.basicConfig(format='%(message)s', level=logging.INFO)

    logging.info("Starting Container %s (%s)..\n", CONTAINER_NAME,
                                                   CONTAINER_VERSION)

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3',
                             region_name='eu-central-1')
    sqs_client = boto3.client('sqs',
                              region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    # Initialize Application class
    selector = Selector(sqs_client, container_services)

    # Main loop
    while(True):
        #selector.handle_selector_queue()
        selector.handle_hq_queue()

        

if __name__ == '__main__':
    main()