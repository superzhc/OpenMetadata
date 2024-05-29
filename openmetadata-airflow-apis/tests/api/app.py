import os
from flask import Flask, request, jsonify

os.environ["FLASK_ENV"] = "development"

app = Flask(__name__)


@app.before_request
def filter1():
    print(f"Enter: {request.url}")


@app.route("/")
def index():
    return "<h1>Openmetadata Ingestion</h1>"


@app.route("/json")
def json_api():
    return jsonify(author="superz", env="dev")


if __name__ == "__main__":
    app.run(
        # debug=True,
    )
