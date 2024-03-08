# app.py

import json

from flask import Flask
from flask import jsonify
from flask import render_template


app = Flask(__name__)

# Setup logging and log timestamp prepend
import logging


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.database import TimeScaleService


def get_latest() -> list[dict]:
    """Get latest value for each indicator."""
    return TimeScaleService().execute(
        query="""SELECT DISTINCT ON (component_name) component_name, value
            FROM indicator_results
            ORDER BY component_name, time DESC
        """
    )


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/sma")
def sma():
    return render_template("sma.html")


@app.route("/latest", methods=["GET"])
def get_latest_data():
    data = get_latest()
    results = {}
    for row in data:
        results[row["component_name"]] = json.loads(row["value"])

    if data:
        return jsonify(results)
    else:
        return jsonify({"error": "No data available"})


if __name__ == "__main__":
    app.run(debug=True)
