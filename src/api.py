from flask import Flask, request, jsonify, render_template

from src.process_data import Processor
from src.load_data import DataLoader
from src.log import Logger

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/load_source', methods=['GET'])
def load_source():
    Logger.clear_log()
    try:
        Logger.log("Loading source data...")
        dl = DataLoader()
        dl.load_source_ddl()
        dl.load_marts_ddl()
        dl.load_data()
        Logger.log("Done.")
    except Exception as e:
        return render_template('index.html', error=str(e))
    return render_template('index.html', log=Logger.read_log())

@app.route('/erase_data', methods=['GET'])
def erase_data():
    Logger.clear_log()
    try:
        p = Processor()
        p.erase_all_data()
    except Exception as e:
        return render_template('index.html', error=str(e))
    return render_template('index.html', log=Logger.read_log())


@app.route('/load_data', methods=['GET'])
def load_data():
    Logger.clear_log()
    try:
        p = Processor()
        p.load_all_data()
    except Exception as e:
        return render_template('index.html', error=str(e))
    return render_template('index.html', log=Logger.read_log())


@app.route('/get_data_mart', methods=['GET'])
def get_data_mart():
    try:
        dl = DataLoader()
        dm = dl.read_data_mart()
        return render_template('index.html', data=dm)
    except Exception as e:
        return render_template('index.html', error=str(e))

