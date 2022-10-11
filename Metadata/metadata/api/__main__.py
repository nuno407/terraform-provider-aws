"""
Metadata API
"""
import logging
import os

from metadata.api.controller import app
from base.aws.container_services import ContainerServices

# DocumentDB info
DB_NAME = "DB_data_ingestion"

if __name__ == '__main__':

    # Define configuration for logging messages
    _logger = ContainerServices.configure_logging('metadata_api')

    # Start API process
    if os.getenv('LOCAL_DEBUG', False):
        app.run("127.0.0.1", port=7777, use_reloader=True, debug=True)
    else:
        from waitress import serve
        serve(app, listen="*:5000", url_scheme="https")
