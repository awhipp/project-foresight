# app.py

import json
import os

import dotenv
from flask import Flask
from flask import jsonify
from flask import render_template

from foresight.utils.database import TimeScaleService
from foresight.utils.logger import generate_logger


app = Flask(__name__)

# Load environment variables
dotenv.load_dotenv()

logger = generate_logger(name=__name__)


def get_latest() -> list[dict]:
    """Get latest value for each indicator."""
    return TimeScaleService().execute(
        query="""SELECT DISTINCT ON (component_name) component_name, value
            FROM indicator_results
            ORDER BY component_name, time DESC
        """,
    )


@app.route("/")
def home():
    """Render the home page."""
    return render_template("index.html")


@app.route("/sma")
def sma():
    """Render the simple-moving average page."""
    return render_template("sma.html")


@app.route("/latest", methods=["GET"])
def get_latest_data():
    """Get the latest data."""
    data = get_latest()
    results = {}
    for row in data:
        results[row["component_name"]] = json.loads(row["value"])

    if data:
        return jsonify(results)
    else:
        return jsonify({"error": "No data available"})


if __name__ == "__main__":
    debug_mode = os.getenv("APP_DEBUG", "False").lower() == "true"
    app.run(debug=debug_mode)
