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
from flask_restx import Api, Resource, reqparse, fields

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v6.0"

# DocumentDB info
DB_NAME = "DB_test"

# AWS region 
REGION_NAME = "eu-central-1"

# API response codes
ERROR_HTTP_CODE = '500'
SUCCESS_HTTP_CODE = '200'
BAD_REQUEST_CODE = '400'
NOT_FOUND_CODE = '404'

# API response messages
ERROR_400_MSG = 'Invalid or missing argument(s)'
ERROR_500_MSG = 'Mapping Key Error'

CUSTOM_MSG = "TO BE CHANGED"

# API instance initialisation
app = flask.Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Swagger documentation initialisation
api = Api(app, version='1.0', title='Metadata management API',
    description='API used for communication between Fronted UI and DocumentDB database',
)

# Common models used in most endpoints
error_400_model = api.model("Error_400", {
    'message': fields.String(example=ERROR_400_MSG),
    'statusCode': fields.String(example="400")
})

error_500_model = api.model("Error_500", {
    'message': fields.String(example=ERROR_500_MSG),
    'statusCode': fields.String(example="500")
})

def create_mongo_client():
    """
    Creates Mongo client to access DocDB cluster

    Returns:
        client {pymongo.mongo_client.MongoClient'} -- [Mongo client used to
                                                       access AWS DocDB cluster]
    """
    client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=docdb_info['username'],
                         password=docdb_info['password'],
                         tls=docdb_info['tls'],
                         tlsCAFile=docdb_info['tlsCAFile'],
                         replicaSet=docdb_info['replicaSet'],
                         readPreference=docdb_info['readPreference'],
                         retryWrites=docdb_info['retryWrites']
                        )
    return client

@api.route('/alive')
class Alive(Resource):
    @api.doc(responses={200:'Ok'})
    def get(self):
        """
        Checks if API is alive
        """
        return flask.jsonify(message='Ok', statusCode="200")

@api.route('/ready')
class Ready(Resource):
    @api.doc(responses={200:'Ready'})
    def get(self):
        """
        Checks if API is ready
        """
        return flask.jsonify(message='Ready', statusCode="200")

@api.route('/dbStatus')
class Status(Resource):
    @api.doc(responses={200:CUSTOM_MSG, 400:ERROR_400_MSG, 500:ERROR_500_MSG})
    def get(self):
        """
        Returns a list of all databases and collections currently present on the DocumentDB cluster
        ** FOR DEBUG PURPOSES **
        """
        try:
            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Get list of current databases on the cluster
            response = {}
            response['dbs_list'] = client.list_database_names()

            # Get list of current collections on each database
            response['col_list'] = {}
            for db_name in response['dbs_list']:
                mydb = client[db_name]
                response['col_list'][db_name] = mydb.list_collection_names()

            # Close the connection
            client.close()

            return flask.jsonify(message=response, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Parameters parser for debugAddItem endpoint (Swagger documentation)
add_one_parser = reqparse.RequestParser()
add_one_parser.add_argument('item', type=str, required=True, help='Item to be added to a given collection', location='form')
add_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection where the item is going to be added', location='form')

# Custom model for debugAddItem code 200 response (Swagger documentation)
add_item_200_model = api.model("Add_item_200", {
    'message': fields.String(example="Added item: {'_id': 'Mary', 'address': 'Highway 99'}"),
    'statusCode': fields.String(example="200")
})

@api.route('/debugAddItem')
class AddItem(Resource):
    @api.response(200, 'Success', add_item_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(add_one_parser, validate=True)
    def post(self):
        """
        Inserts item in a given collection
        ** FOR DEBUG PURPOSES **

        """
        try:
            # Get info attached to request
            str_item = flask.request.form["item"]
            collection = flask.request.form["collection"]

            # Converts item received from string to dict
            new_body = str_item.replace("\'", "\"")
            item = json.loads(new_body)

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            # Insert item
            x = col.insert_one(item)

            # Close the connection
            client.close()

            response_msg = 'Added item: {}'.format(str(item))
            return flask.jsonify(message=response_msg, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Parameters parser for getAllItems endpoint (Swagger documentation)
get_all_parser = reqparse.RequestParser()
get_all_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to get all items', location='form')

# Custom model for getAllItems code 200 response (Swagger documentation)
get_all_200_model = api.model("Add_item_200", {
    'message': fields.List(fields.Raw([{"_id": "John","address": "Highway 2"},{"_id": "Jack","address": "Highway 2"}])),
    'statusCode': fields.String(example="200")
})

@api.route('/getAllItems')
class GetAll(Resource):
    @api.response(200, 'Success', get_all_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(get_all_parser, validate=True)
    def post(self):
        """
        Returns all items present in a given collection
        """
        try:
            # Get info attached to request
            collection = flask.request.form["collection"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            # Get all info from the table
            response_msg = list(col.find({}))

            # Close the connection
            client.close()

            return flask.jsonify(message=response_msg, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")










##############################################################################################################

@app.route("/getItem", methods=["POST"])
def get_one_item():
    """
    Returns status code 200 if get is successfull
    It will query a DocumentDB collection to show a given item in that
    collection

    Arguments:
    Returns:
        flask.jsonify -- [json with the status code + data]
    """

    if flask.request.method == "POST":

        if flask.request.form.get("value") and flask.request.form.get("collection") and flask.request.form.get("parameter"):

            # Get info attached to request
            value = flask.request.form["value"]
            collection = flask.request.form["collection"]
            parameter = flask.request.form["parameter"]

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
                # Find the document with request id
                response = col.find_one({parameter:value})
                # Close the connection
                client.close()
                return flask.jsonify(code=SUCCESS_HTTP_CODE, message=response)
            except:
                # Close the connection
                client.close()
                # Return error code 404 if no item found
                # TODO: CHANGE THIS ERROR HANDLING PART
                response_msg = 'No item with requested id was found in collection {}'.format(collection)
                response = flask.jsonify(code=NOT_FOUND_CODE, message=response_msg)
                response.status_code = 404
                return response

    # Return error code 400 if one or more parameters are missing
    response_msg = 'One or more request parameters missing!'
    response = flask.jsonify(code=BAD_REQUEST_CODE, message=response_msg)
    response.status_code = 400
    return response

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
                # TODO: CHECK FIRST IF ITEM EXISTS 
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

    # Build connection info to access DocDB cluster
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
