import os
import flask
import queue
import logging

app = flask.Flask(__name__)

pipeline = queue.Queue(maxsize=10)

@app.route("/alive", methods=["GET"])
def alive():
    return flask.jsonify(code='200', message='Ok')


@app.route("/ready", methods=["GET"])
def ready():
    return flask.jsonify(code='200', message='Ready')

@app.route("/feature_chain", methods=["POST"])
def chain_producer():
    '''
    if flask.request.method == "POST":
        if flask.request.files.get("video"):

            chunk = flask.request.files["video"]
            chunk_info = secure_filename(chunk.filename)
            chunk.save(chunk_info)

        logging.info("Producer got video path to: %s", chunk_info)
        pipeline.put(chunk_info)
    '''
    logging.info("Producer received event. Exiting")

    return flask.jsonify(code='200', message='Handling video in Chain in a minute')


if __name__ == '__main__':
    # First almost working draft
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    app.run("0.0.0.0", use_reloader=True, debug=False)     

