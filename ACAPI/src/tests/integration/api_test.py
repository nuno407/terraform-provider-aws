"""
This file has the responsability of
doing the API the integration API tests
to ensure that everything is working in
the cluster.
"""

import logging
import requests
'''
IP_SVC = '10.142.226.82'
PORT_POD = '5000'
REQ_COMMAND = 'alive'
ADDR = 'http://{}:{}/{}'.format(IP_SVC, PORT_POD, REQ_COMMAND)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    try:
        logging.info("Starting request!")
        logging.info(ADDR)
        r = requests.get(ADDR)
        logging.info(r.content)
        if r.status_code == 200:
            print("Passed...!")
        else:
            print("Failed...!")
    except requests.exceptions.ConnectionError as error:
        logging.error(error)
'''