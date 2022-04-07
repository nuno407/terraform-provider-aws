"""
Metadata API 
"""
import logging
import os

from api.controller import app

# DocumentDB info
DB_NAME = "DB_data_ingestion"

if __name__ == '__main__':

    # Define configuration for logging messages
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")

    # Start API process
    if os.getenv('LOCAL_DEBUG', False):
        app.run("127.0.0.1", port=7777, use_reloader=True, debug=True)
    else:
        app.run("0.0.0.0", port=5000, use_reloader=False, debug=False)
    
