import json
import logging

from flask import Flask, Response, request
from flask_cors import CORS

from db import DBPool
from settings import DB_HOST, DB_PORT, DB_NAME, APP_HOST, APP_PORT
from src.logger import configure_logger
from src.validator import check_filter_request

logger = logging.getLogger(__file__)
logger = configure_logger(logger)

# start application
app = Flask(__name__)
db = DBPool(DB_HOST, DB_PORT)
db.connect(DB_NAME)
CORS(app)

FILTER_PARAMS = {'univs': 'Університет', 'knowledge_areas': 'Галузь знань',
                 'years': 'Рік', 'type.school_score': 'По балам атестату',
                 'regions': 'Регіон', 'type.gov_exams': 'По балам ЗНО',
                 'part_top_applicants.value': 'ТОП студентів(%)'}


@app.route('/', methods=['GET'])
def get_filtering_params():
    knowledge_areas = db.get_knowledge_areas()
    regions = db.get_regions()
    university_titles = db.get_university_titles()
    response_dict = {
        'tags': FILTER_PARAMS,
        'area_title': knowledge_areas,
        'univ_location': regions,
        'univ_title': [x['univ_title'] for x in university_titles]
    }
    response = Response(
        json.dumps(response_dict),
        mimetype='application/json')
    response.headers['Access-Control-Allow-Origin'] = '*'
    logger.info(f'[GET] Response\n{response.json}')
    return response


@app.route('/', methods=['POST'])
def filter_data_and_analyse():
    data = request.get_json()
    logger.info(f'[POST] Received\n{data}')
    data = check_filter_request(data)
    if data.filter.univ_titles:
        data.filter.univ_ids = db.get_universities_by_titles(
            data.filter.univ_titles)
        data.filter.univ_titles = None
    requests = db.get_requests_by_filter(data.filter)
    univs = db.get_universities_by_requests(requests)
    univ_ids = [univ['univ_id'] for univ in univs]
    labels_fields = {
        'average_overall_score': 'total_score',
        'average_school_score': 'school_score'
    }
    additional_info = db.get_additional_data_by_univ(
        univ_ids, labels_fields)
    for info, univ in zip(additional_info, univs):
        info.update(univ)
        del info['_id']
    result = sorted(additional_info, key=lambda x: x['average_overall_score'],
                    reverse=True)
    return Response(json.dumps(result), mimetype='application/json')


if __name__ == '__main__':
    app.debug = True
    app.run(host=APP_HOST, port=APP_PORT)
