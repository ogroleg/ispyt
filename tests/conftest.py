import pytest
from flask import Flask
from flask_cors import CORS

from db import DBPool
from settings import DB_HOST, DB_PORT, DB_NAME, APP_HOST, APP_PORT


@pytest.fixture
def database():
    db = DBPool(DB_HOST, DB_PORT)
    db.connect(DB_NAME)
    yield db


@pytest.fixture
def app():
    app = Flask(__name__)
    CORS(app)
    app.debug = True
    app.run(host=APP_HOST, port=APP_PORT)
    yield app
