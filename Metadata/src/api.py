"""
This file should be used to create an API
to communicate between the application and
the database.
"""
import logging
import flask
from botocore.exceptions import ClientError
from flask_cors import CORS
from pymongo import MongoClient
import json

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v6.0"

# DocumentDB info
DB_NAME = "DB_test"
EXEC_COL_NAME = "dev-pipeline-execution"
ALGO_COL_NAME = "dev-algorithm-output"

# AWS region 
REGION_NAME = "eu-central-1"

# API response codes
ERROR_HTTP_CODE = "500"
SUCCESS_HTTP_CODE = "200"
BAD_REQUEST_CODE = "400"
NOT_FOUND_CODE = "404"

app = flask.Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route("/alive", methods=["GET"])
def alive():
    """
    Returns status code 200 if alive

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    return flask.jsonify(code=SUCCESS_HTTP_CODE, message='Ok')

@app.route("/ready", methods=["GET"])
def ready():
    """
    Returns status code 200 if ready

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    return flask.jsonify(code=SUCCESS_HTTP_CODE, message='Ready')

@app.route("/getAllData", methods=["GET"])
def get_all_data():
    """
    Returns status code 200 if get is successfull
    It will query a DocumentDB table to show everything in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    # Create a MongoDB client, open a connection to Amazon DocumentDB
    # as a replica set and specify the read preference as
    # secondary preferred
    client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=docdb_info['username'],
                         password=docdb_info['password'],
                         tls=docdb_info['tls'],
                         tlsCAFile=docdb_info['tlsCAFile'],
                         replicaSet=docdb_info['replicaSet'],
                         readPreference=docdb_info['readPreference'],
                         retryWrites=docdb_info['retryWrites']
                        )

    # Specify the database to be used
    db = client[DB_NAME]

    ##Specify the collection to be used
    col = db[EXEC_COL_NAME]

    try:
        # Get all info from the table
        response = list(col.find({}))
        '''
        response = []
        for x in col.find():
            response.append(x)
        '''
        # Close the connection
        client.close()
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/getAllResults", methods=["GET"])
def get_all_results():
    """
    Returns status code 200 if get is successfull
    It will query a DocumentDB table to show everything in that
    table

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    # Create a MongoDB client, open a connection to Amazon DocumentDB
    # as a replica set and specify the read preference as
    # secondary preferred
    client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=docdb_info['username'],
                         password=docdb_info['password'],
                         tls=docdb_info['tls'],
                         tlsCAFile=docdb_info['tlsCAFile'],
                         replicaSet=docdb_info['replicaSet'],
                         readPreference=docdb_info['readPreference'],
                         retryWrites=docdb_info['retryWrites']
                        )

    # Specify the database to be used
    db = client[DB_NAME]

    ##Specify the collection to be used
    col = db[ALGO_COL_NAME]

    try:
        # Get all info from the table
        response = list(col.find({}))
        '''
        response = []
        for x in col.find():
            response.append(x)
        '''
        # Close the connection
        client.close()
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/getExecItem", methods=["POST"])
def get_exec_item():
    """
    Returns status code 200 if get is successfull
    It will query a DocumentDB collection to show a given item in that
    collection

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """

    if flask.request.method == "POST":

        if flask.request.form.get("id"):
            # Get info attached to request
            item_id = flask.request.form["id"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = MongoClient(docdb_info['cluster_endpoint'], 
                                 username=docdb_info['username'],
                                 password=docdb_info['password'],
                                 tls=docdb_info['tls'],
                                 tlsCAFile=docdb_info['tlsCAFile'],
                                 replicaSet=docdb_info['replicaSet'],
                                 readPreference=docdb_info['readPreference'],
                                 retryWrites=docdb_info['retryWrites']
                                )

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[EXEC_COL_NAME]

            try:
                # Find the document with request id
                response = col.find_one({'id':item_id})
                # Close the connection
                client.close()
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
            except:
                # Close the connection
                client.close()
                # Return error code 404 if no item found
                response_msg = 'No item with requested id was found in collection'
                response = flask.jsonify(code=NOT_FOUND_CODE, message=response_msg)
                response.status_code = 404
                return response

    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code=BAD_REQUEST_CODE, message=response_msg)
    response.status_code = 400
    return response

@app.route("/getAlgoItem", methods=["POST"])
def get_algo_item():
    """
    Returns status code 200 if get is successfull
    It will query a DocumentDB collection to show a given item in that
    collection

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """

    if flask.request.method == "POST":

        if flask.request.form.get("id"):
            # Get info attached to request
            item_id = flask.request.form["id"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = MongoClient(docdb_info['cluster_endpoint'], 
                                 username=docdb_info['username'],
                                 password=docdb_info['password'],
                                 tls=docdb_info['tls'],
                                 tlsCAFile=docdb_info['tlsCAFile'],
                                 replicaSet=docdb_info['replicaSet'],
                                 readPreference=docdb_info['readPreference'],
                                 retryWrites=docdb_info['retryWrites']
                                )

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[ALGO_COL_NAME]

            try:
                # Find the document with request id
                response = col.find_one({'run_id':item_id})
                # Close the connection
                client.close()
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
            except:
                # Close the connection
                client.close()
                # Return error code 404 if no item found
                response_msg = 'No item with requested id was found in collection'
                response = flask.jsonify(code=NOT_FOUND_CODE, message=response_msg)
                response.status_code = 404
                return response

    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code=BAD_REQUEST_CODE, message=response_msg)
    response.status_code = 400
    return response

@app.route("/dbStatus", methods=["GET"])
def get_db_status():
    """
    Returns status code 200 if get is successfull
    ** FOR DEBUG PURPOSES **

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    # Create a MongoDB client, open a connection to Amazon DocumentDB
    # as a replica set and specify the read preference as
    # secondary preferred
    client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=docdb_info['username'],
                         password=docdb_info['password'],
                         tls=docdb_info['tls'],
                         tlsCAFile=docdb_info['tlsCAFile'],
                         replicaSet=docdb_info['replicaSet'],
                         readPreference=docdb_info['readPreference'],
                         retryWrites=docdb_info['retryWrites']
                        )

    try:
        response = {}

        # Get list of current databases on the cluster
        response['dbs_list'] = client.list_database_names()


        logging.info(response['dbs_list'])


        # Get list of current collections on each database
        response['col_list'] = {}
        for db_name in response['dbs_list']:
            mydb = client[db_name]
            response['col_list'][mydb] = mydb.list_collection_names()


            logging.info(mydb.list_collection_names())

        logging.info("---------------------------------")
        logging.info(response)
        print(response)
        
        
        client.close()
        return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
    except ClientError as error:
        return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/debugAddItem", methods=["POST"])
def debug_add_item():
    """
    Returns status code 200 if get is successfull
    ** FOR DEBUG PURPOSES **

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    if flask.request.method == "POST":

        if flask.request.form.get("item") and flask.request.form.get("collection"):

            # Get info attached to request
            str_item = flask.request.form["item"]
            collection = flask.request.form["collection"]

            # Converts item received from string to dict
            new_body = str_item.replace("\'", "\"")
            item = json.loads(new_body)

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = MongoClient(docdb_info['cluster_endpoint'], 
                                username=docdb_info['username'],
                                password=docdb_info['password'],
                                tls=docdb_info['tls'],
                                tlsCAFile=docdb_info['tlsCAFile'],
                                replicaSet=docdb_info['replicaSet'],
                                readPreference=docdb_info['readPreference'],
                                retryWrites=docdb_info['retryWrites']
                                )

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            try:
                # Insert item
                x = col.insert_one(item)

                # Close the connection
                client.close()
                response_msg = 'Added item: {}'.format(str(item))
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response_msg)
            except ClientError as error:
                return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/debugDeleteAll", methods=["POST"])
def debug_delete_all():
    """
    Returns status code 200 if get is successfull
    ** FOR DEBUG PURPOSES **

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    if flask.request.method == "POST":

        if flask.request.form.get("collection"):

            # Get info attached to request
            collection = flask.request.form["collection"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = MongoClient(docdb_info['cluster_endpoint'], 
                                username=docdb_info['username'],
                                password=docdb_info['password'],
                                tls=docdb_info['tls'],
                                tlsCAFile=docdb_info['tlsCAFile'],
                                replicaSet=docdb_info['replicaSet'],
                                readPreference=docdb_info['readPreference'],
                                retryWrites=docdb_info['retryWrites']
                                )

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            try:
                # Delete items
                x = col.delete_many({})

                # Close the connection
                client.close()
                response_msg = 'Deleted all items from collection: {}'.format(str(collection))
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response_msg)
            except ClientError as error:
                return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])

@app.route("/debugDeleteItem", methods=["POST"])
def debug_delete_item():
    """
    Returns status code 200 if get is successfull
    ** FOR DEBUG PURPOSES **

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """
    if flask.request.method == "POST":

        if flask.request.form.get("item") and flask.request.form.get("collection"):

            # Get info attached to request
            str_item = flask.request.form["item"]
            collection = flask.request.form["collection"]

            # Converts item received from string to dict
            new_body = str_item.replace("\'", "\"")
            item = json.loads(new_body)

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = MongoClient(docdb_info['cluster_endpoint'], 
                                username=docdb_info['username'],
                                password=docdb_info['password'],
                                tls=docdb_info['tls'],
                                tlsCAFile=docdb_info['tlsCAFile'],
                                replicaSet=docdb_info['replicaSet'],
                                readPreference=docdb_info['readPreference'],
                                retryWrites=docdb_info['retryWrites']
                                )

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            try:
                # Delete item
                col.delete_one(item)

                # Close the connection
                client.close()
                response_msg = 'Deleted item: {}'.format(str(item))
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response_msg)
            except ClientError as error:
                return flask.jsonify(code=ERROR_HTTP_CODE, message=error.response['Error']['Message'])


if __name__ == '__main__':

    # Define configuration for logging messages
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")

    # Build connection string to access DocDB cluster
    # DO NOT CHANGE THE docdb_info PARAMETERS ORDER!
    '''
    connection_string = 'mongodb://'
    docdb_info = {
                  'sample-user': "usertest1",
                  '1st_marker': ":",
                  'password': "pass-test",
                  '2nd_marker': "@",
                  'sample-cluster': "docdb-cluster-demo-0",
                  '3rd_maker': ".czddtysxwqch.",
                  'region': REGION_NAME,
                  '4th_marker': ".docdb.amazonaws.com:27017/",
                  'ssl_config': "?ssl=true",
                  'ssl_ca_certs_config': "&ssl_ca_certs=rds-combined-ca-bundle.pem",
                  'retry_config': "&retryWrites=false"
                 }

    for i in docdb_info.keys():
        connection_string += docdb_info[i]
    '''
    docdb_info = {
                  'cluster_endpoint': 'docdb-cluster-demo.cluster-czddtysxwqch.eu-central-1.docdb.amazonaws.com',
                  'username': 'usertest1',
                  'password': 'pass-test',
                  'tls': 'true',
                  'tlsCAFile': 'rds-combined-ca-bundle.pem',
                  'replicaSet': 'rs0',
                  'readPreference': 'secondaryPreferred',
                  'retryWrites': 'false'
                }

    # Start API process
    app.run("0.0.0.0", use_reloader=True, debug=True)
