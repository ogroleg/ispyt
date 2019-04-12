import logging
from typing import Optional

from src.exceptions import InvalidRequestParameter
from src.logger import configure_logger
from src.model import FilterRequest, Filter, PART_TOP_TYPES, LENGTHS

logger = logging.getLogger(__file__)
logger = configure_logger(logger)

YEARS = [2014, 2015, 2016, 2017, 2018]

ERRORS = {
    'check_min_length': 'value is too short',
    'check_max_length': 'value is too long',
    'field_missing': 'field is required. Field is missing in  request',
    'check_boolean': 'field must be boolean ("true"/"false")',
    'unknown_value': 'Cannot recognize field value'
}


def check_filter_request(request_json: dict) -> Optional[FilterRequest]:
    try:
        filter_request = FilterRequest(request_json)
        check_filter(filter_request.filter)
        return filter_request
    except Exception as e:
        logger.error(e)


def check_filter(request: Filter):
    if request.part_top_applicants is not None and \
            'type' in request.part_top_applicants:
        if request.part_top_applicants['type'] not in PART_TOP_TYPES:
            raise InvalidRequestParameter(
                'Unknown part of top applications type:'
                f' {request.part_top_applicants["type"]}')


def check_filtering_params(data):
    status = {'status': True, 'error': []}
    keys = ['univ_title', 'area_title', 'univ_location', 'year',
            'is_enrolled', 'part_top_applicants']
    for key in keys:
        if key == 'univs' or key == 'region' or key == 'knowledge_areas':
            check_min_length_for_array(data, key, status['error'])
            check_max_length_for_array(data, key, status['error'])
        elif key == 'is_enrolled' and (not is_boolean(data[key])):
            status['error'].append({key: ERRORS['check_boolean']})
        elif key == 'part_top_applicants':
            status['error'] += check_part_top_applicants(data[key])
    return status


def check_part_top_applicants(data):
    error = []
    keys = ['type', 'value']
    for key in keys:
        if key not in data:
            error.append({key: ERRORS['field_missing']})
        elif key == 'type' and data[key] in PART_TOP_TYPES:
            error.append({key: ERRORS['unknown_value']})
        elif key == 'value' and not between(0, data[key], 100):
            error.append({key: ERRORS['unknown_value']})
    return error


def check_min_length_for_array(data, key, error):
    is_valid_min_length = False not in [
        check_minimum_length(s, LENGTHS[key][0]) for s in data[key]]
    if not is_valid_min_length:
        error.append({key: ERRORS['check_min_length']})


def check_max_length_for_array(data, key, error):
    is_valid_max_length = False not in [
        check_maximum_length(s, LENGTHS[key][1]) for s in data[key]]
    if not is_valid_max_length:
        error.append({key: ERRORS['check_max_length']})


def check_minimum_length(string, minimum):
    """Validator function which checks if string is bigger than
       minimum value.
       :params: string - string to check
                minimum - minimal length
    """
    return len(str(string)) >= minimum


def check_maximum_length(string, maximum):
    """Validator function which checks if string is bigger than
       minimum value.
       :params: string - string to check
                minimum - minimal length
    """
    return len(str(string)) <= maximum


def is_boolean(string):
    return string == "true" or string == "false"


def between(low: int, value, high: int):
    return low <= int(value) <= high
