from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import dill
import numpy as np
import pandas as pd
import flask

# Settings
PIPELINE_PATH = "/app/app/models/pipeline.dill"
LOG_PATH = "app.log"
PORT = 8180

# Globals
pipeline = None

# Flask
app = flask.Flask("sber_client_age")

# Logging
handler = RotatingFileHandler(filename=LOG_PATH, maxBytes=100000, backupCount=10)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# pandas dataframe from request_json
def get_dataframe(request_json, pipeline):
    _names = pipeline[0].frame_names  # stored during fit
    keys = [_names['index']] + _names['columns']
    frame_dict = {col: request_json[col] for col in keys}
    return pd.DataFrame(frame_dict).set_index(_names['index'])

@app.route("/", methods=["GET"])
def general():    
    return f"Welcome to the client age predictions. Please use {flask.request.host_url}predict to POST"

@app.route("/predict", methods=["POST"])
def predict():
    response = {'success': False}
    if flask.request.method == "POST":
        request_json = flask.request.get_json()
        try:
            test_df = get_dataframe(request_json, pipeline)
            test_clients = ', '.join(map(str, test_df.index.unique()))
            y_pred = pipeline.predict(test_df)
            response = {
                'success': True,
                'predictions': y_pred.tolist()
            }
            logger.info(f'{datetime.utcnow().isoformat()} Clients: {test_clients}')
        except AttributeError as e:
            logger.warning(f'{datetime.utcnow().isoformat()} Exception: {str(e)}')
            response['predictions'] = str(e)
            
    return flask.jsonify(response)
    

if __name__ == "__main__":
    global pipepine
    
    print("Loading API, please wait...")
    
    with open(PIPELINE_PATH, "rb") as f:
        pipeline = dill.load(f)
        print(pipeline)
    
    app.run(host='0.0.0.0', debug=True, port=PORT)