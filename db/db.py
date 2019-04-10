import logging

from pymongo import MongoClient

from data_parser.properties import DB_CONNECTION_TIMEOUT
from utils import generate_id

logger = logging.getLogger(__name__)

FILTER_KEYS = ['univ_title', 'area_title',
               {'part_of_applicants': ['type', 'value']},
               'univ_location', 'year', 'is_enrolled']


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
        except Exception as e:
            logger.error(e)
            logger.error(f'Failed to write data: {result}')


class DBPool(object):
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._db = None

    def connect(self, database: str):
        client = MongoClient(self._host, self._port)
        self._db = client[database]

    def get_university_titles(self):
        return list(self._db.univs.find({}, {'_id': 0, 'univ_title': 1}))

    def get_knowledge_areas(self):
        return list(self._db.areas.find({}, {'area_title': 1})
                    .distinct('area_title'))

    @staticmethod
    def _remove_none_from_list(values):
        return [x for x in values if x]

    def _get_area_id_by_title(self, title):
        return self._db.areas.find({"area_title": title}, {"_id": 1})

    def get_regions(self):
        values = list(self._db.univs.find({}, {'univ_location': 1})
                      .distinct('univ_location'))
        return DBPool._remove_none_from_list(values)

    def get_regions_by_filter(self, filter_data):
        print(filter_data)
        filter_data = self._get_regions_by_filter(filter_data)
        print(filter_data)
        resulting_data = self._db.requests.find(filter_data, {"_id": 0})
        return list(resulting_data)

    def _get_regions_by_filter(self, filter_data: dict):
        result_query = {}
        if 'area_title' in filter_data:
            values = filter_data.pop('area_title')
            result_query['course_id'] = {
                "$in": self.get_courses_by_filter({'area_title': values})}
        result_query.update(
            self._format_filter_data_to_mongo_request(filter_data))
        return result_query

    def _format_filter_data_to_mongo_request(self, data):
        result_query = {}
        for key in FILTER_KEYS:
            if isinstance(key, dict) or key == 'univ_location':
                # print(isinstance(key, dict), key)
                continue
            if key in data:
                print(key)
                if key == 'univ_title':
                    if 'univ_location' in data:
                        univ_ids = self._get_univ_ids(
                            data[key], data['univ_location'])
                    else:
                        univ_ids = [generate_id(title) for title in data[key]]
                    if len(univ_ids) > 0:
                        result_query['univ_id'] = {"$in": univ_ids}
                elif key == 'area_title':
                    result_query['area_id_old'] = {
                        "$in": [generate_id(title) for title in data[key]]}
                else:
                    if key == 'is_enrolled':
                        result_query[key] = data[key] == 'true'
                    else:
                        result_query[key] = {"$in": data[key]}
        return result_query

    def get_courses_by_filter(self, filter_data):
        filter_data = self._format_filter_data_to_mongo_request(filter_data)
        return self._db.courses.find(filter_data, {"_id": 0})

    def _get_univ_ids(self, title, locations):
        return list(self._db.univs.find(
            {'univ_title': {"$in": title},
             'univ_location': {"$in": locations}},
            {"univ_id": 1}).distinct('univ_id'))

    def get_university_title_by_id(self, univ_id):
        return list(self._db.univs.find(
            {"univ_id": univ_id},
            {"_id": 0, "univ_id": 1}).dictinct("univ_id"))
