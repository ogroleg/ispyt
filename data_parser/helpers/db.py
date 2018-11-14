from pymongo import MongoClient

from .properties import DB_CONNECTION_TIMEOUT


def connect_to_database(hostname, database):
    return MongoClient(host=hostname, connectTimeoutMS=DB_CONNECTION_TIMEOUT).get_database(database)


def save_results_to_db(db, results):
    """
    Saves data after processing to db.
    :param db: active connection to db
    :param results: list
    """
    db.requests.insert(results)
