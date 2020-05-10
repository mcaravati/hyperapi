"""
    A simple HyperPlanning API since the official HyperPlanning
    website is dynamic and difficult to scrape
"""
import json
from datetime import datetime, timedelta
import flask
from flask_cors import CORS
from flask import jsonify
import databasemanager

DB = databasemanager.DatabaseManager("database.db")
DB.build()

APP = flask.Flask(__name__)
CORS(APP)


@APP.route('/', methods=['GET'])
def home():
    """
    Displays the default welcome message.

    :return:
        str
    """
    return flask.render_template('index.html')


@APP.route('/api/s2/<group>/<period>', defaults={'bounds': None})
@APP.route('/api/s2/<group>/<period>/<bounds>', methods=['GET'])
def second_semester(group: str, period: str, bounds: str):
    """
    Returns the lessons of the day, of the week, or of a specific date.
    Expects an address of the following format:
    hyperapi.hubday.fr/api/s2/<group>/<period>[/<bounds>]

    :param group: The group you want to get the lessons of
    :param period: The desired period type
    :param bounds: The bounds of the period
    :return:
        Json or str
    """

    result = None

    if period == "today":
        result = jsonify(json.loads(
            DB.get_sql(group,
                       day=(datetime.now() + timedelta(hours=1)).date())))
    elif period == "week":
        result = jsonify(json.loads(
            DB.get_sql(group,
                       week=bounds)))
    elif period == "day":
        result = jsonify(json.loads(
            DB.get_sql(group,
                       day=(datetime.strptime(bounds, "%Y-%m-%d").date()))))

    return "Error while parsing request, check request syntax" if result is None else result


APP.run(host='0.0.0.0', port=8080)
