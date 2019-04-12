import logging
from functools import lru_cache
from itertools import chain

from pymongo import MongoClient

from data_parser.properties import DB_CONNECTION_TIMEOUT
from src.logger import configure_logger
from src.model import Filter
from utils import generate_id, decode_id

logger = logging.getLogger(__name__)
logger = configure_logger(logger)

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
        self.host = host
        self.port = port
        self.db = None
        self.requests = None

    def connect(self, database: str):
        client = MongoClient(self.host, self.port)
        self.db = client[database]
        self.requests = self.db.requests

    def get_university_titles(self):
        return list(self.db.univs.find({}, {'_id': 0, 'univ_title': 1}))

    def get_knowledge_areas(self):
        return list(self.db.areas.find({}, {'area_title': 1})
                    .distinct('area_title'))

    @staticmethod
    def _remove_none_from_list(values):
        return [x for x in values if x]

    def _get_area_id_by_title(self, title):
        return self.db.areas.find({"area_title": title}, {"_id": 1})

    def get_regions(self):
        values = list(self.db.univs.find({}, {'univ_location': 1})
                      .distinct('univ_location'))
        return DBPool._remove_none_from_list(values)

    def get_regions_by_filter(self, filter_data: dict):
        logger.debug(filter_data)
        filter_data = self._get_regions_by_filter(filter_data)
        logger.debug(filter_data)
        resulting_data = self.db.requests.find(filter_data, {"_id": 0})
        logger.debug(resulting_data)
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
                logger.debug(
                    f'Omitting filtering by univ_location or dict: {key}')
                continue
            if key in data:
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
        return self.db.courses.find(filter_data, {"_id": 0})

    def _get_univ_ids(self, title, locations):
        return list(self.db.univs.find(
            {'univ_title': {"$in": title},
             'univ_location': {"$in": locations}},
            {"univ_id": 1}).distinct('univ_id'))

    def get_university_title_by_id(self, univ_id):
        return list(self.db.univs.find(
            {"univ_id": univ_id},
            {"_id": 0, "univ_id": 1}).dictinct("univ_id"))

    @lru_cache()
    def get_university_ids_by_region(self, region):
        return [obj['univ_id'] for obj in self.db.univs.find(
            {'univ_location': region})]

    def get_university_ids_by_regions(self, regions: list):
        return list(chain(*[self.get_university_ids_by_region(region)
                            for region in regions]))

    @lru_cache(maxsize=10000)
    def get_course_ids_by_area_title(self, area_title):
        area_id = generate_id(area_title)

        return [generate_id(obj['course_title'])
                for obj in self.db.courses.find({'area_id_old': area_id})]

    def get_course_ids_by(self, area_titles: list):
        return list(chain(*[self.get_course_ids_by_area_title(area_title)
                            for area_title in area_titles]))

    def get_requests_by_filter(self, request: Filter):
        query = {}
        if request.univ_ids:
            query['univ_id'] = {'$in': request.univ_ids}
        if request.knowledge_areas:
            query['course_id'] = {
                '$in': self.get_course_ids_by(request.knowledge_areas)}
        if request.regions:
            if 'univ_id' not in query:
                query['univ_id'] = {'$in': list()}
            query['univ_id']['$in'].extend(
                self.get_university_ids_by_regions(request.regions))
        if request.enrolled_only is not None:
            query['is_enrolled'] = bool(request.enrolled_only)
        logger.debug(query)
        return list(self.requests.find(query))

    @lru_cache()
    def get_university_by_id(self, univ_id):
        result = self.db.univs.find_one({'univ_id': univ_id})
        del result['_id']
        return result

    def get_universities_by_ids(self, univ_ids):
        return [self.get_university_by_id(univ_id)
                for univ_id in sorted(univ_ids)]

    @lru_cache()
    def get_university_by_title(self, univ_title):
        result = self.db.univs.find_one({'univ_title': univ_title})
        return result['univ_id']

    def get_universities_by_titles(self, univ_titles):
        return [self.get_university_by_title(univ_title)
                for univ_title in sorted(univ_titles)]

    def get_universities_by_requests(self, requests):
        univ_ids = set(x['univ_id'] for x in requests)
        return self.get_universities_by_ids(univ_ids)

    def get_additional_data_by_univ(self, univ_ids: list,
                                    average_fields: dict,
                                    enrolled_only: bool = True):
        match = {'univ_id': {'$in': univ_ids}}
        group = {'_id': "$univ_id", 'count': {'$sum': 1},
                 'total_data': {'$push': '$total_score'}}
        if enrolled_only:
            match['is_enrolled'] = True
            group['passing_overall_score'] = {'$min': f'$total_score'}
        for label, field in average_fields.items():
            group[label] = {'$avg': f'${field}'}
        pipeline = [{'$match': match}, {'$group': group},
                    {'$sort': {'_id': 1}}]
        return list(self.requests.aggregate(pipeline))

    def get_knowledge_areas_by_university(self, univ_title: str) -> list:
        univ_id = self.get_university_by_title(univ_title)
        courses = self.requests.distinct('course_id', {'univ_id': univ_id})
        courses = [decode_id(course) for course in courses]
        logger.debug(courses)
        result = self.db.courses.distinct(
            'area_id_old', {'course_title': {'$in': courses}})
        return [decode_id(title) for title in result]
