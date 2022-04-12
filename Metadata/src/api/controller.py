"""
Metadata API 
"""
import logging
import flask
from flask import make_response, request
from flask_cors import CORS
from flask_restx import Api, Resource, reqparse, fields
from api.config import service

class ReverseProxied(object):
    # Inspired by: http://flask.pocoo.org/snippets/35/
    def __init__(self, wsgi_app, app):
        self.wsgi_app = wsgi_app
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '/api')
        if script_name:
            if script_name.startswith('/'):
                environ['SCRIPT_NAME'] = script_name
                path_info = environ['PATH_INFO']
                if path_info.startswith(script_name):
                    environ['PATH_INFO'] = path_info[len(script_name):]
            else:
                self.app.warning("'prefix' must start with a '/'!")

        return self.wsgi_app(environ, start_response)

# API response messages
ERROR_400_MSG = 'Invalid or missing argument(s)'
ERROR_404_MSG = 'Method not found'
ERROR_500_MSG = 'Internal Server Error'

# API instance initialisation
app = flask.Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app, app)
CORS(app, resources={r"/*": {"origins": "*"}})

# Swagger documentation initialisation
api = Api(app, version='1.0', title='Metadata management API',
        description='API used for communication between Frontend UI and DocumentDB database',
        default="General endpoints", default_label="")

# Create namespace for debug endpoints
ns = api.namespace('Debug endpoints', description="", path="/")

def generate_exception_logs():
    """
    Generates exception logs to be displayed during container execution
    """
    logging.info("\n######################## Exception #########################")
    logging.exception("The following exception occured during execution:")
    logging.info("############################################################\n")



# Error 404 general handler
@app.errorhandler(404)
def not_found(_):
    return make_response(flask.jsonify(message=ERROR_404_MSG, statusCode="404"), 404)

# Common models used in most endpoints
error_400_model = api.model("Error_400", {
    'message': fields.String(example=ERROR_400_MSG),
    'statusCode': fields.String(example="400")
})

# Common models used in most endpoints
error_404_model = api.model("Error_404", {
    'message': fields.String(example=ERROR_404_MSG),
    'statusCode': fields.String(example="404")
})

error_500_model = api.model("Error_500", {
    'message': fields.String(example=ERROR_500_MSG),
    'statusCode': fields.String(example="500")
})


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


# Parameters parser for getVideoUrl endpoint (Swagger documentation)
video_parser = reqparse.RequestParser()
video_parser.add_argument('bucket', type=str, required=True, help='S3 bucket where the video file is located', location='args')
video_parser.add_argument('folder', type=str, required=True, help='S3 folder where the video file is located', location='args')
video_parser.add_argument('file', type=str, required=True, help='Name of the video file', location='args')

# Custom model for getVideoUrl code 200 response (Swagger documentation)
get_video_200_model = api.model("Video_Url_200", {
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
        """
        Gets the URL of a video file for direct access or embedding as a video stream
        """
        try:
            url  = service.create_video_url(bucket, folder, file)
            return flask.jsonify(message=url, statusCode="200")
        except (NameError, LookupError, ValueError):
            generate_exception_logs()
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except Exception:
            generate_exception_logs()
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Parameters parser for getVideoSignals endpoint (Swagger documentation)
videosignals_parser = reqparse.RequestParser()
videosignals_parser.add_argument('video_id', type=str, required=True, help='Name of the video file', location='args')

# Custom model for getVideoUrl code 200 response (Swagger documentation)
get_videosignals_200_model = api.model("Video_Signals_200", {
    'message': fields.String(example="<recording_id>"),
    'statusCode': fields.String(example="200")
})

@api.route('/getVideoSignals/<string:video_id>')
class VideoSignals(Resource):
    @api.response(200, 'Success', get_videosignals_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(videosignals_parser, validate=True)
    def get(self, video_id):
        """
        Gets all the signals that are recorded with the video (MDF) or postprocessed afterwards (eg CHC)
        """
        try:
            video_signals = service.get_video_signals(video_id)
            return flask.jsonify(message=video_signals, statusCode="200")
        except (NameError, LookupError, ValueError):
            generate_exception_logs()
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except Exception:
            generate_exception_logs()
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")
            
# Custom model for getTableData code 200 response (Swagger documentation)
tabledata_nest_model = api.model("tabledata_nest", {
    'item_1_id': fields.String(example="recording_overview: [recording_id, algo_processed, snapshots, CHC_events, lengthCHC, status, length, time,"),
    'item_2_id': fields.String(example="recording_overview: ['deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1637316243575_1637316303540', 'True', '5', '15' , '00:30:00', status, '00:09:00', 23-11-2021 14:32, 600x480, 'yuj2hi_01_InteriorRecorder']"),
})

get_tabledata_200_model = api.model("Get_tabledata_200", {
    'message': fields.Nested(tabledata_nest_model),
    'pages': fields.Integer(example=5),
    'total': fields.Integer(example=95),
    'statusCode': fields.String(example="200")
})
@api.route('/getTableData')
class TableData(Resource):
    @api.response(200, 'Success', get_tabledata_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self):
        """
        Gets the recording overview list so it can be viewed in Recording overview table in the Front End
        """
        # Get the query parameters from the request
        page_size = request.args.get('size', 20, int)
        page = request.args.get('page', 1, int)


        try:
            response_msg, number_recordings, number_pages = service.get_table_data(page_size, page)
            return flask.jsonify(message=response_msg, pages=number_pages, total=number_recordings, statusCode="200")
        except (NameError, LookupError, ValueError):
            generate_exception_logs()
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except Exception:
            generate_exception_logs()
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

get_single_tabledata_200_model = api.model("Get_single_tabledata_200", {
    'message': fields.String("recording_overview: ['deepsensation_ivs_slimscaley_develop_yuj2hi_01_InteriorRecorder_1637316243575_1637316303540', 'True', '5', '15' , '00:30:00', status, '00:09:00', 23-11-2021 14:32, 600x480, 'yuj2hi_01_InteriorRecorder']"),
    'statusCode': fields.String(example="200")
})

# Parameters parser for getTableData endpoint (Swagger documentation)
tabledata_parser = reqparse.RequestParser()
tabledata_parser.add_argument('requested_id', type=str, required=True, help='Name of the video file', location='args')
@api.route('/getTableData/<requested_id>')
class SingleTableData(Resource):
    @api.response(200, 'Success', get_single_tabledata_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    @api.expect(tabledata_parser, validate=True)
    def get(self, requested_id):
        """
        Gets the recording for the Recording detail page in the Front End
        """
        try:
            response_msg = service.get_single_recording(requested_id)
            return flask.jsonify(message=response_msg, statusCode="200")
        except (NameError, LookupError, ValueError):
            generate_exception_logs()
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except Exception:
            generate_exception_logs()
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

# Custom model for getAnonymizedVideoUrl code 200 response (Swagger documentation)
get_anonymized_video_url_200_model = api.model("get_anonymized_video_url_200", {
    'message': fields.String(example="https://qa-rcd-anonymized-video-files.s3.amazonaws.com/Debug_Lync/srxfut2internal23_rc_srx_qa_eur_fut2_009_InteriorRecorder_1644054706267_1644054801665_anonymized.mp4"),
    'statusCode': fields.String(example="200")
})

@api.route('/getAnonymizedVideoUrl/<recording_id>')
class VideoUrl(Resource):
    """
    Gets the URL of an anonymized version of a video file for direct access or embedding as a video stream
    """
    @api.response(200, 'Success', get_anonymized_video_url_200_model)
    @api.response(400, ERROR_400_MSG, error_400_model)
    @api.response(500, ERROR_500_MSG, error_500_model)
    def get(self, recording_id):
        """
        Returns the video URL available for one DB item
        """
        try:
            video_url = service.create_anonymized_video_url(recording_id)
            return flask.jsonify(message=video_url, statusCode="200")
        except (NameError, LookupError, ValueError):
            generate_exception_logs()
            api.abort(400, message=ERROR_400_MSG, statusCode = "400")
        except Exception:
            generate_exception_logs()
            api.abort(500, message=ERROR_500_MSG, statusCode = "500")

