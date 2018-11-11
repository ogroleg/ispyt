from pymongo import MongoClient

from data_parser.properties import DB_CONNECTION_TIMEOUT


def connect_to_database(hostname, database):
    return MongoClient(host=hostname, connectTimeoutMS=DB_CONNECTION_TIMEOUT).get_database(database)


def save_results_to_db(db, results):
    pass
