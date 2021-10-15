"""
Metadata API 
"""
import logging
import flask
from flask_cors import CORS
from pymongo import MongoClient
import json
from flask_restx import Api, Resource, reqparse, fields

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v7.0"

# DocumentDB info
DB_NAME = "DB_test"

# API response messages
ERROR_400_MSG = 'Invalid or missing argument(s)'
ERROR_500_MSG = 'Mapping Key Error'

# API instance initialisation
app = flask.Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Swagger documentation initialisation
api = Api(app, version='1.0', title='Metadata management API',
          description='API used for communication between Fronted UI and DocumentDB database',
          default="General endpoints", default_label="")

# Create namespace for debug endpoints
ns = api.namespace('Debug endpoints', description="", path="/")

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

# Custom model for alive code 200 response (Swagger documentation)
alive_200_model = api.model("Alive_200", {
    'message': fields.String(example="Ok"),
    'statusCode': fields.String(example="200")
})

@api.route('/alive')
class Alive(Resource):
    @api.response(200, 'Success', alive_200_model)
    def get(self):
        """
        Checks if API is alive
        """
        return flask.jsonify(message='Ok', statusCode="200")

# Custom model for ready code 200 response (Swagger documentation)
ready_200_model = api.model("Ready_200", {
    'message': fields.String(example="Ready"),
    'statusCode': fields.String(example="200")
})

@api.route('/ready')
class Ready(Resource):
    @api.response(200, 'Success', ready_200_model)
    def get(self):
        """
        Checks if API is ready
        """
        return flask.jsonify(message='Ready', statusCode="200")

# Custom models for dbStatus code 200 response (Swagger documentation)
sub_nest_model = api.model("sub_nest", {
    'DB_test': fields.Raw(["dev-algorithm-output", "dev-pipeline-execution"])
})
nest_model = api.model("nest", {
    'dbs_list': fields.List(fields.Raw("DB_test")),
    'col_list': fields.Nested(sub_nest_model)
})
status_200_model = api.model("status", {
    'message': fields.Nested(nest_model),
    'statusCode': fields.String(example="200")
})
   
@api.route('/dbStatus')
class Status(Resource):
    @api.response(200, 'Success', status_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Returns a list of all databases and collections currently present on the DocumentDB cluster
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
add_one_200_model = api.model("Add_one_200", {
    'message': fields.String(example="Added item: {'_id': 'Mary', 'address': 'Highway 99'}"),
    'statusCode': fields.String(example="200")
})

@ns.route('/debugAddItem')
class AddItem(Resource):
    @ns.response(200, 'Success', add_one_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(add_one_parser, validate=True)
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
get_all_200_model = api.model("Get_all_200", {
    'message': fields.Raw([{"_id": "John","address": "Highway 2"},{"_id": "Jack","address": "Highway 2"}]),
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

# Parameters parser for getItem endpoint (Swagger documentation)
get_one_parser = reqparse.RequestParser()
get_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to get an item', location='form')
get_one_parser.add_argument('value', type=str, required=True, help='Value that specified parameter should have', location='form')
get_one_parser.add_argument('parameter', type=str, required=True, help='Parameter to use to search for specific item', location='form')

# Custom model for getItem code 200 response (Swagger documentation)
get_nest_model = api.model("Get_nest_200", {
    '_id': fields.String(example="Mary"),
    'address': fields.String(example="Highway 99")
})
get_one_200_model = api.model("Get_one_200", {
    'message': fields.Nested(get_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getItem')
class GetOne(Resource):
    @api.response(200, 'Success', get_one_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(get_one_parser, validate=True)
    def post(self):
        """
        Returns the item from a given collection that has the specific value for a given parameter
        """
        try:
            # Get info attached to request
            value = flask.request.form["value"]
            collection = flask.request.form["collection"]
            parameter = flask.request.form["parameter"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            # Find the document with request id
            response_msg = col.find_one({parameter:value})

            # Close the connection
            client.close()

            return flask.jsonify(message=response_msg, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Parameters parser for debugDeleteAll endpoint (Swagger documentation)
del_all_parser = reqparse.RequestParser()
del_all_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to delete all items', location='form')

# Custom model for debugDeleteAll code 200 response (Swagger documentation)
del_all_200_model = ns.model("Del_all_200", {
    'message': fields.String(example="Deleted all items from collection: example-collection"),
    'statusCode': fields.String(example="200")
})

@ns.route('/debugDeleteAll')
class DelAll(Resource):
    @ns.response(200, 'Success', del_all_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(del_all_parser, validate=True)
    def post(self):
        """
        Deletes all items from a given collection
        ** FOR DEBUG PURPOSES **
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

            # Delete items
            x = col.delete_many({})

            # Close the connection
            client.close()

            response_msg = 'Deleted all items from collection: {}'.format(str(collection))
            return flask.jsonify(message=response_msg, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Parameters parser for debugDeleteItem endpoint (Swagger documentation)
del_one_parser = reqparse.RequestParser()
del_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to delete a given item', location='form')
del_one_parser.add_argument('item', type=str, required=True, 
                            help='Item to be deleted (the value of this parameter could be just the parameter-value pair of the unique key)', location='form')

# Custom model for debugDeleteItem code 200 response (Swagger documentation)
del_one_200_model = api.model("Del_one_200", {
    'message': fields.String(example="Deleted item: {'_id': 'Jack'}"),
    'statusCode': fields.String(example="200")
})

@ns.route('/debugDeleteItem')
class DelOne(Resource):
    @ns.response(200, 'Success', del_one_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(del_one_parser, validate=True)
    def post(self):
        """
        Deletes one item from a given collection
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

            # Delete item
            # TODO: CHECK FIRST IF ITEM EXISTS 
            col.delete_one(item)

            # Close the connection
            client.close()

            response_msg = 'Deleted item: {}'.format(str(item))
            return flask.jsonify(message=response_msg, statusCode="200")
        except Exception as e:
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except KeyError as e:
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")


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
