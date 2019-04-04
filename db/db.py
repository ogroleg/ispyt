import logging

from bson import InvalidDocument
from pymongo import MongoClient

from data_parser.properties import DB_CONNECTION_TIMEOUT

logger = logging.getLogger(__name__)


def connect_to_database(hostname, database):
    return MongoClient(host=hostname, connectTimeoutMS=DB_CONNECTION_TIMEOUT) \
        .get_database(database)


def save_results_to_db(db, results):
    """
    Saves data after processing to db.
    :param db: active connection to db
    :param results: list
    """
    for result in results:
        try:
            db.requests.insert(result, check_keys=False)
        except InvalidDocument as e:
            logger.error(e)
            logger.error(f'Failed to write data: {result}')
