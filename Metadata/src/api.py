"""
Metadata API 
"""
import logging
import flask
from flask import make_response
from flask_cors import CORS
from pymongo import MongoClient, collection
import json
from flask_restx import Api, Resource, reqparse, fields
from baseaws.shared_functions import ContainerServices
import boto3
from mongosanitizer.sanitizer import sanitize
import re

# Container info
CONTAINER_NAME = "Metadata"
CONTAINER_VERSION = "v7.3"

# DocumentDB info
DB_NAME = "DB_data_ingestion"

# API response messages
ERROR_400_MSG = 'Invalid or missing argument(s)'
ERROR_404_MSG = 'Method not found'
ERROR_500_MSG = 'Internal Server Error'

# API instance initialisation
app = flask.Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Swagger documentation initialisation
api = Api(app, version='1.0', title='Metadata management API',
          description='API used for communication between Frontend UI and DocumentDB database',
          default="General endpoints", default_label="")

# Create namespace for debug endpoints
ns = api.namespace('Debug endpoints', description="", path="/")

# Error 404 general handler
@app.errorhandler(404)
def not_found(_):
    return make_response(flask.jsonify(message=ERROR_404_MSG, statusCode="404"), 404)

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
    # Load documentDB login info from config file
    docdb_info = container_services.docdb_config

    # Create the necessary client for AWS secrets manager access
    secrets_client = boto3.client('secretsmanager',
                                  region_name=docdb_info['region_name'])

    # Get password and username from secrets manager
    response = secrets_client.get_secret_value(SecretId=docdb_info['secret_name'])
    str_response = response['SecretString']

    # Converts response body from string to dict
    # (in order to perform index access)
    new_body = str_response.replace("\'", "\"")
    dict_response = json.loads(new_body)

    # Mongo client creation with info previously built
    client = MongoClient(docdb_info['cluster_endpoint'], 
                         username=dict_response['username'],
                         password=dict_response['password'],
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
        # ############################################################################################################################################### DEBUG (UPDATE MDF INFO FROM S3 INTO DB)

        # # Create a MongoDB client, open a connection to Amazon DocumentDB
        # # as a replica set and specify the read preference as
        # # secondary preferred
        # client = create_mongo_client()

        # # Specify the database to be used
        # db = client[DB_NAME]

        # ##Specify the collection to be used
        # col = db["dev-recording"]

        # # Get all info from the table
        # response_msg = list(col.find({}))

        # logging.info(response_msg)
        
        # for data_db in response_msg: 

        #     s3_bucket, video_key = data_db["s3_path"].split("/", 1)
        #     s3_key = video_key.split(".")[0] + '_metadata_full.json'

        #     logging.info(s3_bucket)
        #     logging.info(s3_key)

        #     if s3_key == "Debug_Lync/deepsensation_rc_srx_develop_ivs1hi_04_InteriorRecorder_1639492620739_1639492653894_metadata_full.json":
        #         continue

        #     # Download metadata json file
        #     response = s3_client.get_object(
        #                                         Bucket=s3_bucket,
        #                                         Key=s3_key
        #                                     )

        #     # Decode and convert file contents into json format
        #     result_info = json.loads(response['Body'].read().decode("utf-8"))

        #     # Get info to populate CHC blocked events arrays (CHC)
        #     chb_array = []

        #     # Check all frames
        #     for frame in result_info['frame']:
        #         # Validate every frame (check if it has objectlist parameter)
        #         if 'objectlist' in frame.keys():
        #             for item in frame['objectlist']:
        #                 # Check for item with ID = 1
        #                 # (it has the CameraViewBlocked info)
        #                 if item['id'] == '1':
        #                     chb_value = item['floatAttributes'][0]['value']
        #                     chb_array.append(chb_value)
        #         else:
        #             chb_array.append("0")


        #     try: 
        #         for aux in data_db["results_CHC"]:
        #             if aux["source"] == "MDF":
        #                 aux["CHBs"] = chb_array
        #                 break
        #     except Exception as e:
        #         logging.info(e)
        #         continue

        #     col.update_one({'_id': data_db["_id"]}, {"$set": {"results_CHC": data_db["results_CHC"]}}) 

        # # Close the connection
        # client.close()

        # #################################################################################################################################################

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for addItem endpoint (Swagger documentation)
add_one_parser = reqparse.RequestParser()
add_one_parser.add_argument('item', type=str, required=True, help='Item to be added to a given collection', location='form')
add_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection where the item is going to be added', location='args')

# Custom model for addItem code 200 response (Swagger documentation)
add_one_200_model = api.model("Add_one_200", {
    'message': fields.String(example="Added item: {'_id': 'Mary', 'address': 'Highway 99'}"),
    'statusCode': fields.String(example="200")
})

@ns.route('/addItem/<string:collection>')
class AddItem(Resource):
    @ns.response(200, 'Success', add_one_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(add_one_parser, validate=True)
    def post(self, collection):
        """
        Inserts item in a given collection
        ** FOR DEBUG PURPOSES **

        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

            # Get info attached to request
            str_item = flask.request.form["item"]

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for getAllItems endpoint (Swagger documentation)
get_all_parser = reqparse.RequestParser()
get_all_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to get all items', location='args')

# Custom model for getAllItems code 200 response (Swagger documentation)
get_all_200_model = api.model("Get_all_200", {
    'message': fields.Raw([{"_id": "John","address": "Highway 2"},{"_id": "Jack","address": "Highway 2"}]),
    'statusCode': fields.String(example="200")
})

@api.route('/getAllItems/<string:collection>')
class GetAll(Resource):
    @api.response(200, 'Success', get_all_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(get_all_parser, validate=True)
    def get(self, collection):
        """
        Returns all items present in a given collection
        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for getItem endpoint (Swagger documentation)
get_one_parser = reqparse.RequestParser()
get_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to get an item', location='args')
get_one_parser.add_argument('value', type=str, required=True, help='Value that specified parameter should have', location='args')
get_one_parser.add_argument('parameter', type=str, required=True, help='Parameter to use to search for specific item', location='args')

# Custom model for getItem code 200 response (Swagger documentation)
get_nest_model = api.model("Get_nest_200", {
    '_id': fields.String(example="Mary"),
    'address': fields.String(example="Highway 99")
})
get_one_200_model = api.model("Get_one_200", {
    'message': fields.Nested(get_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getItem/<string:collection>/<string:parameter>/<string:value>')
class GetOne(Resource):
    @api.response(200, 'Success', get_one_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(get_one_parser, validate=True)
    def get(self, collection, parameter, value):
        """
        Returns the item from a given collection that has the specific value for a given parameter
        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for getQueryItems endpoint (Swagger documentation)
get_query_parser = reqparse.RequestParser()
get_query_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to get the items', location='args')
get_query_parser.add_argument('query', type=str, required=True, help='DocDB custom pair(s) of parameter:value to use to get items. Use the following format (json): {"parameter1":{"operator1":"value1"}, "parameter2":{"operator2":"value2"}, ... }', location='args')
get_query_parser.add_argument('logic_operator', type=str, required=True, help='DocDB operator used to link multiple queries (supported: AND, OR)', location='args')

get_query_200_model = api.model("Get_query_200", {
    'message': fields.Raw([{"_id": "John","address": "Highway 2"},{"_id": "Jack","address": "Highway 2"}]),
    'statusCode': fields.String(example="200")
})
query_error_400_model = api.model("Get_query_400", {
    'message': fields.String(example="Invalid input format for query. Use the following format for queries: {'parameter1':{'operator1':'value1'}, 'parameter2':{'operator2':'value2'}, ... }"),
    'statusCode': fields.String(example="400")
})

@api.route('/getQueryItems/<string:collection>/<string:query>/<string:logic_operator>')
class GetQuery(Resource):
   @api.response(200, 'Success', get_query_200_model)
   @api.response(400, ERROR_400_MSG, query_error_400_model)
   @api.response(500, ERROR_500_MSG, error_500_model)
   @api.expect(get_query_parser, validate=True)
   def get(self, collection, query, logic_operator):
        """
        Returns all items for a custom query

        ## Example of query request

        If it necessary to query the "dev-algorithm-output" DB collection for items where:
        - The **processing algorithm** was **anonymization** (i.e. parameter "algorithm_id" equals "Anonymize")

        - The **name** of the item contains the word **"pipeline"** (i.e. parameter "_id" has the substring "pipeline)

        then, the request arguments should be the following:  
        - **collection:**  `"dev-algorithm-output"`

        - **query:** `{ 'algorithm_id': {'==' : 'Anonymize'}, '_id': {'has' : 'pipeline'} }`

        - **logic_operator:** `AND` (if both conditions are required) or `OR` (if only one condition needs to be met)
        ---

        ## Currently supported operators (for query argument)

        | Operator | Description |
        | ----------- | ----------- |
        | `==` | Equal |
        | `!=` | Not equal |
        | `>` | Greater than |
        | `<` | Less than |
        | `has` | Contains substring in its value |

        ---

        ## Currently supported logical operators (for logical_operator argument)

        | Operator | Description |
        | ----------- | ----------- |
        | `AND` / `and` |  All parameter:value conditions must be met |
        | `OR` / `or` | At least one parameter:value condition must be met |

        ---
        
        ## Resulting query format

            QUERY: <subquery_1> <logic_operator> <subquery_2> <logic_operator> ...

        where:
        - `<subquery_x>`  -  corresponds to x<sup>th</sup> condition stated on the query argument (e.g. {'parameter_x': {'operator_x' : 'value_x'}} )  

        - `<logic_operator>` - corresponds to the logical operator used link two or more expressions (currently supported logical operators: AND, OR).
        <br/>

        **Note:** The usage of logical operators is limited to one of them (and not both) per request, i.e.:
        - `<subquery_1> AND <subquery_2> AND ...` or `<subquery_1> OR <subquery_2> OR ...` are supported

        - `<subquery_1> OR <subquery_2> AND ...` is not currently supported

        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"
            
            # State all valid logical operators
            valid_logical = {
                              "or":"$or",
                              "and":"$and"
                            }
            # TODO: ADD valid_logical to config file

            # List all valid logical operators
            valid_logic_keys = list(valid_logical.keys())

            # Check if operator is in the valid list
            assert logic_operator.lower() in valid_logic_keys, "Invalid/Forbidden logical operator"

            # Sanitize query to be ready for assertion          
            sanitize(query)

            ## Parameters (keys) validation #################
            
            # Converts query received from string to dict
            new_body = query.replace("\'", "\"")
            json_query = json.loads(new_body)

            # Load whitelist for valid keys from config file
            whitelist = container_services.docdb_whitelist[collection]

            # Create list of keys from received query
            keys_to_check = list(json_query.keys())

            # Check if all keys received are valid
            assert [a for a in keys_to_check if a not in whitelist] == [], "Invalid/Forbidden query keys"

            ## Split processing and validation(operations + values) ################

            # State all valid query operators and their
            # corresponding pymongo operators (used for
            # validation and later conversion)
            valid_ops_dict = {
                              "==":"$eq",
                              ">":"$gt",
                              "<":"$lt",
                              "!=":"$nin",
                              "has":"$regex"
                            }
            # TODO: ADD valid_ops_dict to config file
            
            # Create list of valid operators for query validation
            valid_ops_keys = list(valid_ops_dict.keys())

            # Create empty list that will contain all sub queries received
            query_list = []

            for key in keys_to_check:
                # Get item (pair op/value) for a given key (parameter)
                op_value = list(json_query[key].items())[0]

                ## OPERATOR VALIDATION + CONVERSION
                # Check if operator is valid 
                assert op_value[0] in valid_ops_keys, "Invalid/Forbidden query operators"

                # Convert the operator to pymongo syntax
                ops_conv = valid_ops_dict[op_value[0]]

                ## VALUE VALIDATION
                # Check if value is valid (i.e. alphanumeric and/or with characters _ : . -)
                # NOTE: No spaces are allowed in the value string!
                assert re.findall("^[a-zA-Z0-9_:.-]*$", str(op_value[1])) != [], "Invalid/Forbidden query values"

                # Convert value to array if operation is $nin
                # TODO: CHECK IF THIS STEP IS NECESSARY
                if ops_conv == "$nin":
                    # Create subquery. 
                    # NOTE: $exists -> used to make sure items without
                    # the parameter set in key are not also returned
                    subquery = {key:{'$exists': 'true', ops_conv:[op_value[1]]}}
                else:
                    # Create subquery. 
                    # NOTE: $exists -> used to make sure items without
                    # the parameter set in key are not also returned
                    subquery = {key:{'$exists': 'true', ops_conv:op_value[1]}}

                # Append subquery to list
                query_list.append(subquery)

            # Append selected logical operator to query
            # NOTE: Resulting format -> { <AND/OR>: [<subquery1>, <subquery2>, ...] },
            #       which translates into: <subquery1> AND/OR <subquery2> AND/OR ... 
            query_request = {valid_logical[logic_operator]: query_list}

            ## DocDB connection ###################
            
            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            # Find the documents that match the query conditions
            response_msg = list(col.find(query_request))

            # Close the connection
            client.close()

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for deleteAllItems endpoint (Swagger documentation)
del_all_parser = reqparse.RequestParser()
del_all_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to delete all items', location='args')

# Custom model for deleteAllItems code 200 response (Swagger documentation)
del_all_200_model = ns.model("Del_all_200", {
    'message': fields.String(example="Deleted all items from collection: example-collection"),
    'statusCode': fields.String(example="200")
})

@ns.route('/deleteAllItems/<string:collection>')
class DelAll(Resource):
    @ns.response(200, 'Success', del_all_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(del_all_parser, validate=True)
    def delete(self, collection):
        """
        Deletes all items from a given collection
        ** FOR DEBUG PURPOSES **
        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for deleteItem endpoint (Swagger documentation)
del_one_parser = reqparse.RequestParser()
del_one_parser.add_argument('collection', type=str, required=True, help='DocDB Collection from where to delete a given item', location='args')
del_one_parser.add_argument('item', type=str, required=True, 
                            help='Item to be deleted (the value of this parameter could be just the parameter-value pair of the unique key)',
                            location='form')

# Custom model for deleteItem code 200 response (Swagger documentation)
del_one_200_model = api.model("Del_one_200", {
    'message': fields.String(example="Deleted item: {'_id': 'Jack'}"),
    'statusCode': fields.String(example="200")
})

@ns.route('/deleteItem/<string:collection>')
class DelOne(Resource):
    @ns.response(200, 'Success', del_one_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(del_one_parser, validate=True)
    def delete(self, collection):
        """
        Deletes one item from a given collection
        ** FOR DEBUG PURPOSES **
        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

            # Get info attached to request  
            str_item = flask.request.form["item"]

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
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for deleteCollection endpoint (Swagger documentation)
del_col_parser = reqparse.RequestParser()
del_col_parser.add_argument('collection', type=str, required=True, help='DocDB Collection to be deleted', location='args')

# Custom model for deleteCollection code 200 response (Swagger documentation)
del_col_200_model = ns.model("Del_col_200", {
    'message': fields.String(example="Deleted collection: example-collection"),
    'statusCode': fields.String(example="200")
})

@ns.route('/deleteCollection/<string:collection>')
class DelCol(Resource):
    @ns.response(200, 'Success', del_col_200_model)
    @ns.response(400, ERROR_400_MSG, error_400_model)
    @ns.response(500, ERROR_500_MSG, error_500_model)
    @ns.expect(del_col_parser, validate=True)
    def delete(self, collection):
        """
        Deletes a given collection
        ** FOR DEBUG PURPOSES **
        """
        try:
            # Load list of collections currently available on DocDB
            valid_collections = list(container_services.docdb_whitelist.keys())
            # TODO: USE METHOD SIMILAR TO DBSTATUS ENDPOINT INSTEAD OF GETTING
            #       INFO FROM CONFIG FILE?? 

            # Check if collection is on the valid list
            assert collection in valid_collections, "Invalid/Forbidden collection"

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection]

            # Delete items
            col.drop()

            # Close the connection
            client.close()

            response_msg = 'Deleted collection: {}'.format(str(collection))
            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for getVideoUrl endpoint (Swagger documentation)
video_parser = reqparse.RequestParser()
video_parser.add_argument('bucket', type=str, required=True, help='S3 bucket where the video file is located', location='args')
video_parser.add_argument('folder', type=str, required=True, help='S3 folder where the video file is located', location='args')
video_parser.add_argument('file', type=str, required=True, help='Name of the video file', location='args')

# Custom model for getVideoUrl code 200 response (Swagger documentation)
get_video_200_model = api.model("Del_one_200", {
    'message': fields.String(example="<video_url>"),
    'statusCode': fields.String(example="200")
})

@api.route('/getVideoUrl/<string:bucket>/<string:folder>/<string:file>')
class VideoFeed(Resource):
    @api.response(200, 'Success', get_video_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(video_parser, validate=True)
    def get(self, bucket, folder, file):
        try:
            # Joins folder and file args to form S3 path
            key = folder + '/' + file

            # Builds params argument
            params_s3 = {'Bucket': bucket, 'Key': key}

            # Request to get video file url
            response_msg  = s3_client.generate_presigned_url('get_object',
                                                             Params = params_s3)

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")


# Custom model for getAllCHBs code 200 response (Swagger documentation)
chb_nest_model = api.model("chb_nest", {
    'item_1_id': fields.String(example="[0.0015,0.002,0.005,0.2]"),
    'item_2_id': fields.String(example="[0.001,0.02,0.035,0.5]"),
})

get_chbs_200_model = api.model("Get_chbs_200", {
    'message': fields.Nested(chb_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getAllCHBs')
class AllCHBs(Resource):
    @api.response(200, 'Success', get_chbs_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Returns the video Camara Healthcheck Blocks available for each DB item
        """
        try:
            # Define DB collection to get recording info from
            collection_rec = container_services.db_tables["recording"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection_rec]

            # Get all info from the table with output video available
            items_list = list(col.find())
            # TODO: DEFINE A BETTER APPROACH TO FIND ALL VIDEOS AVAILABLE

            # Close the connection
            client.close()

            # Iterate received items and Camera HealthChecks blocks for each one
            response_msg = {}

            #validar um video de cada vez            
            for item in items_list:
                chb_dict = {}
                for CHCs_item in item['results_CHC']:    
                    if (CHCs_item['source'] == "MDF"):
                        #logging.info(CHCs_item['source'])                
                        chb_dict[CHCs_item['source']] = CHCs_item['CHBs']
                    else:    
                        #logging.info(CHCs_item['algo_out_id'].split('_')[-1])                
                        chb_dict[CHCs_item['algo_out_id'].split('_')[-1]] = CHCs_item['CHBs']

                response_msg[item['_id']] = chb_dict

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Custom model for getCHBData code 200 response (Swagger documentation)
chbdata_nest_model = api.model("chbdata_nest", {
    'item_1_id': fields.String(example="{'0:00:01:750000': 0.00265299,'0:00:02:666000': 0.00325521}"),
    'item_2_id': fields.String(example="{'0:00:00:750000': 0.00333597,'0:00:01:582000': 0.00404188}"),
})

get_chbdata_200_model = api.model("Get_chbdata_200", {
    'message': fields.Nested(chbdata_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getCHBData')
class CHBData(Resource):
    @api.response(200, 'Success', get_chbdata_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Returns the video timestamps and corresponding Camera View Blocked values
        available for each DB item
        """
        try:
            # Define DB collection to get recording info from
            collection_rec = container_services.db_tables["recording"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            # Specify the collection to be used
            col = db[collection_rec]

            # Get all recording items available on DB
            items_list = list(col.find())

            # Close the connection
            client.close()

            # Dictionary to be returned as API response message
            response_msg = {}

            # Collect dictionary containing timestamps + CHBs values from each item         
            for item in items_list:
                # Create temporary dictionary that will hold the sync data
                # of a given item
                chb_dict = {}

                for CHCs_item in item['results_CHC']: 
                    try:  
                        if CHCs_item['source'] == "MDF":
                            # Retrieve sync info from MDF source
                            chb_dict["MDF"] = CHCs_item['CHBs_sync']
                        else:    
                            # Retrieve sync info from other sources               
                            chb_dict[CHCs_item['algo_out_id'].split('_')[-1]] = CHCs_item['CHBs_sync']
                    except Exception as e:
                        pass

                response_msg[item['_id']] = chb_dict

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

# Parameters parser for getVideoCHC endpoint (Swagger documentation)
videochc_parser = reqparse.RequestParser()
videochc_parser.add_argument('videoID', type=str, required=True, help='Name of the video file', location='args')

# Custom model for getVideoUrl code 200 response (Swagger documentation)
get_videochc_200_model = api.model("Video_CHC_200", {
    'message': fields.String(example="<recording_id>"),
    'statusCode': fields.String(example="200")
})

@api.route('/getVideoCHC/<string:videoID>')
class VideoCHC(Resource):
    @api.response(200, 'Success', get_videochc_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(videochc_parser, validate=True)
    def get(self, videoID):
        try:
            # Define DB collection to get recording info from
            collection_rec = container_services.db_tables["recording"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection_rec]

            # Get all info from the table with output video available
            item = col.find_one({"_id" : videoID})
            # TODO: DEFINE A BETTER APPROACH TO FIND ALL VIDEOS AVAILABLE

            # Close the connection
            client.close()

            # Iterate received items and Camera HealthChecks blocks for each one
            response_msg = {}

            for CHCs_item in item['results_CHC']:
                if (CHCs_item['source'] == "MDF"):
                    #logging.info(CHCs_item['source'])                
                    response_msg[CHCs_item['source']] = CHCs_item['CHBs']
                else:    
                    #logging.info(CHCs_item['algo_out_id'].split('_')[-1])                
                    response_msg[CHCs_item['algo_out_id'].split('_')[-1]] = CHCs_item['CHBs']

                #logging.info(response_msg)

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")


# Custom model for getTableData code 200 response (Swagger documentation)
tabledata_nest_model = api.model("tabledata_nest", {
    'item_1_id': fields.String(example="recording_overview: [recording_id, algo_processed, snapshots, CHC_events, lengthCHC, status, length, time,"),
    'item_2_id': fields.String(example="recording_overview: ['deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1637316243575_1637316303540', 'True', '5', '15' , '00:30:00', status, '00:09:00', 23-11-2021 14:32, 600x480, 'yuj2hi_01_InteriorRecorder']"),
})

get_tabledata_200_model = api.model("Get_tabledata_200", {
    'message': fields.Nested(tabledata_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getTableData')
class TableData(Resource):
    @api.response(200, 'Success', get_tabledata_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Returns the recording overview parameter available for each DB item so it can be viewed in Recording overview table in the Front End
        """
        try:
            # Define DB collection to get recording info from
            collection_results = container_services.db_tables["recording"]

            # Define DB collection to get pipeline info from
            collection_pipe = container_services.db_tables["pipeline_exec"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection_pipe]

            # Get all videos that entered processing phase
            pipe_items_list = list(col.find({"data_status":"complete"}))
            

            # Iterate received items and add additional data from algo and recording databases
            response_msg = []

            #logging.info(pipe_items_list)


            for item in pipe_items_list:
                table_data_dict = {}
                col = db[collection_results]
                # Get the recording data for the video
                record_item_details = col.find_one({"_id":item['_id']})
                
                #logging.info(record_item_details)

                #logging.info(item['_id'].split("_",1)[0])
                #logging.info(item['processing_list'])
                #logging.info(record_item_details['recording_overview']['#snapshots'])

                table_data_dict['number_CHC_events'] = ''      
                table_data_dict['lengthCHC'] = ''
                for chc_result in record_item_details['results_CHC']:
                    try:
                        number_chc, duration_chc = calculate_chc_events(chc_result['CHC_periods'])
                        table_data_dict['number_CHC_events'] = number_chc
                        table_data_dict['lengthCHC'] = duration_chc
                    
                    except Exception as e:
                        #logging.info("No CHC periods present")
                        pass


                #logging.info(item['data_status'])                
                #logging.info(item['last_updated'].split(".",1)[0].replace("T"," "))
                #logging.info(record_item_details['recording_overview']['length'])
                #logging.info(record_item_details['recording_overview']['time'])                
                #logging.info(record_item_details['recording_overview']['resolution'])        
                #logging.info(record_item_details['recording_overview']['deviceID'])     

           
                #Add the fields in the array in the proper order
                table_data_dict['tenant'] = item['_id'].split("_",1)[0]
                table_data_dict['_id'] = item['_id']
                table_data_dict['processing_list'] = item['processing_list']
                table_data_dict['snapshots'] = record_item_details['recording_overview']['#snapshots']
                table_data_dict['data_status'] = item['data_status']                
                table_data_dict['last_updated'] = item['last_updated'].split(".",1)[0].replace("T"," ")
                table_data_dict['length'] = record_item_details['recording_overview']['length']
                table_data_dict['time'] = record_item_details['recording_overview']['time']                
                table_data_dict['resolution'] = record_item_details['recording_overview']['resolution']        
                table_data_dict['deviceID'] = record_item_details['recording_overview']['deviceID']      

                #logging.info(table_data_dict)

                response_msg.append(table_data_dict)


            # Close the connection
            client.close()

            #logging.info(response_msg)
           
            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")

def calculate_chc_events(chc_periods):
    duration = 0.0
    number = 0
    for period in chc_periods:
        duration += period['duration']
        if period['duration'] > 0.0:
            number += 1

    return number, duration


# Custom model for getAllUrls code 200 response (Swagger documentation)
url_nest_model = api.model("url_nest", {
    'item_1_id': fields.String(example="<video_url_1>"),
    'item_2_id': fields.String(example="<video_url_2>"),
})

get_urls_200_model = api.model("Get_urls_200", {
    'message': fields.Nested(url_nest_model),
    'statusCode': fields.String(example="200")
})

@api.route('/getAllUrls')
class AllUrls(Resource):
    @api.response(200, 'Success', get_urls_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Returns the video URL available for each DB item
        """
        try:
            # Define DB collection to get algo output info from
            collection_algo = container_services.db_tables["algo_output"]

            # Create a MongoDB client, open a connection to Amazon DocumentDB
            # as a replica set and specify the read preference as
            # secondary preferred
            client = create_mongo_client()

            # Specify the database to be used
            db = client[DB_NAME]

            ##Specify the collection to be used
            col = db[collection_algo]

            # Get all info from the table with output video available
            items_list = list(col.find({"algorithm_id":"Anonymize"}))
            # TODO: DEFINE A BETTER APPROACH TO FIND ALL VIDEOS AVAILABLE

            # Close the connection
            client.close()

            # Iterate received items and get video url for each one
            response_msg = {}

            for algo_item in items_list:
                # Get video path and split it into bucket and key
                s3_path = algo_item['output_paths']['video']
                bucket, key = s3_path.split("/", 1)

                # Builds params argument
                params_s3 = {'Bucket': bucket, 'Key': key}

                # Request to get video file url
                video_url  = s3_client.generate_presigned_url('get_object',
                                                              Params = params_s3)

                response_msg[algo_item['pipeline_id']] = video_url         

            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError) as e:
            logging.info(e)
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
        except AssertionError as e:
            logging.info(e)
            api.abort(400, message=str(e), statusCode = "400")
        except Exception as e:
            logging.info(e)
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")


if __name__ == '__main__':

    # Define configuration for logging messages
    logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO,
                        datefmt="%H:%M:%S")

    # Create the necessary clients for AWS services access
    s3_client = boto3.client('s3',
                             region_name='eu-central-1')

    # Initialise instance of ContainerServices class
    container_services = ContainerServices(container=CONTAINER_NAME,
                                           version=CONTAINER_VERSION)

    # Load global variable values from config json file (S3 bucket)
    container_services.load_config_vars(s3_client)

    # Start API process
    app.run("0.0.0.0", use_reloader=True, debug=True)
