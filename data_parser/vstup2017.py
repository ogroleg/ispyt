import logging
import multiprocessing
import os
import queue
import threading
from re import sub
from typing import Optional

import requests

from data_parser.AdmissionRequest import AbstractAdmissionRequest, \
    AdmissionRequest2017
from data_parser.InputArgumentParser import create_input_argument_parser
from data_parser.htmlparser.htmlparser2017 import HtmlParser2017
from data_parser.properties import MAX_FILE_CACHE_SIZE, FILE_ENCODING, \
    FILE_CACHE_DELAY, NUM_RESULTS_TO_SAVE
from db.db import connect_to_database, save_results_to_db
from htmlparser.htmlunivparser import get_univ_info_from_page_2017, \
    get_area_course_info
from utils import generate_id

logger = logging.getLogger(__name__)


def get_univ_id_and_list_id_from_filename(file_name: str):
    return list(map(int, file_name[file_name.rindex('i') + 1:-5].split('p')))


def create_request_dictionary_from_raw_data(
        row, request_with_base_data: AbstractAdmissionRequest):
    processed_request = AdmissionRequest2017(request_with_base_data)
    html_parser = HtmlParser2017(row)
    full_name = html_parser.get_full_name()
    try:
        processed_request.set_full_name_and_first_second_last_names(full_name)
        processed_request.extra_points = html_parser.get_extra_points()
        processed_request.rank = html_parser.get_rank()
        processed_request.school_score = \
            html_parser.get_score_of_education_document()
        processed_request.total_score = html_parser.get_total_score()
        processed_request.gov_exams = html_parser.get_government_exam_scores()
        processed_request.priority = html_parser.get_priority()
        processed_request.univ_exams = html_parser.get_university_exams()
        processed_request.coefficients = html_parser.get_coefficients()
        processed_request.is_quota = html_parser.get_is_quota()
        processed_request.is_original = html_parser.get_is_original()
        processed_request.is_enrolled = html_parser.get_is_enrolled()
    except Exception as e:
        logger.error(
            f'i{processed_request.univ_id}p{processed_request.list_id}')
        logger.error(e)
        return None
    return vars(processed_request)


def process_admission_requests(
        requests: [], base_request: AbstractAdmissionRequest) -> list:
    """
    Convert html rows of admission requests to dictionaries
    :param requests: array of str rows of applicant's admission requests
    :param base_request: request object with common data for all requests
    :return: array of dict of requests
    """
    request_dao = [
        create_request_dictionary_from_raw_data(request, base_request)
        for request in requests if request.getchildren()]
    return list(filter(None, request_dao))


def get_common_info_and_create_base_request(
        file_name: str,
        file_string: str) -> Optional[AbstractAdmissionRequest]:
    univ_id, list_id = get_univ_id_and_list_id_from_filename(file_name)
    course_name = HtmlParser2017.get_course(file_string)
    course_id = generate_id(course_name)
    type_of_education = HtmlParser2017.get_type_of_education(file_string)
    if type_of_education is '':
        return
    # if both False - education type is 'дистанційна'
    is_denna = 'денна' in type_of_education or 'вечірня' in type_of_education
    is_zaochna = 'заочна' in type_of_education
    return AbstractAdmissionRequest(
        univ_id, list_id, is_denna, is_zaochna, course_id)


def process_page_with_admission_requests(file_name: str, file_string: str):
    base_request = get_common_info_and_create_base_request(
        file_name, file_string)
    if base_request is None:
        return list()
    requests_body = HtmlParser2017.get_requests_from_page(file_string)
    return process_admission_requests(requests_body, base_request)


def main_worker(files_cache, is_all_files_read, input_arguments):
    db = connect_to_database(input_arguments.db_host, input_arguments.db)
    results = []
    while not is_all_files_read.is_set() or not files_cache.empty():
        try:
            filename, file_string = files_cache.get(timeout=FILE_CACHE_DELAY)
        except queue.Empty:
            continue
        result = process_page_with_admission_requests(filename, file_string)
        if result is None or len(result) == 0:
            continue
        for r in result:
            logger.info(r)
        results.extend(result)
        if len(results) > NUM_RESULTS_TO_SAVE:
            save_results_to_db(db, results)
            results = []
    if results:  # ensure everything is saved
        save_results_to_db(db, results)


def create_queues():
    manager = multiprocessing.Manager()
    files_to_read_queue = manager.Queue()  # queue with inputs
    file_cache = manager.Queue()  # file cache
    is_all_files_read = manager.Event()
    return files_to_read_queue, file_cache, is_all_files_read


def add_files_to_queue(path_to_data, files_to_read):
    for subdir in os.walk(path_to_data):
        files = subdir[-1]
        for x in files:
            if 'p' in x and '.html' in x:
                files_to_read.put((subdir[0], x))


def read_data_from_files_and_add_to_file_cache(
        files_to_read_queue: multiprocessing.Queue,
        file_cache: multiprocessing.Queue):
    while not files_to_read_queue.empty() and \
            file_cache.qsize() < MAX_FILE_CACHE_SIZE:
        try:
            file_path = files_to_read_queue.get_nowait()  # get path to file
        except queue.Empty:
            break  # error getting path, no files left or we should wait
        path = os.path.join(*file_path)
        with open(path, encoding=FILE_ENCODING) as source:  # and read it
            file_data = source.read()
        # store contents in cache
        file_cache.put((os.path.basename(path), file_data))


def read_data_from_files(
        files_to_read_queue: multiprocessing.Queue,
        file_cache: multiprocessing.Queue,
        is_all_files_read: multiprocessing.Event):
    read_data_from_files_and_add_to_file_cache(files_to_read_queue, file_cache)
    if not files_to_read_queue.empty():  # wait and repeat
        start_reading_data_with_delay_in_thread(files_to_read_queue,
                                                file_cache,
                                                is_all_files_read,
                                                FILE_CACHE_DELAY)
    else:  # no files left
        print('stop reading')
        is_all_files_read.set()


def start_reading_data_with_delay_in_thread(
        files_to_read_queue: multiprocessing.Queue,
        file_cache: multiprocessing.Queue,
        are_files_read: multiprocessing.Event,
        delay: float = 0.0):
    t = threading.Timer(delay, read_data_from_files,
                        args=(files_to_read_queue, file_cache, are_files_read))
    t.start()


def start_parsing_pages_and_write_result_to_database(input_arguments):
    if input_arguments.erase:
        db = connect_to_database(input_arguments.db_host, input_arguments.db)
        db.requests.drop()
    path_to_data = input_arguments.path
    files_to_read_queue, file_cache, is_all_files_read = create_queues()
    add_files_to_queue(path_to_data, files_to_read_queue)
    read_data_from_files(files_to_read_queue, file_cache, is_all_files_read)
    print(f'Total files to process: {files_to_read_queue.qsize()}')
    print()
    pool = multiprocessing.Pool(input_arguments.workers)  # create pool
    pool.starmap(main_worker,
                 [(file_cache, is_all_files_read, input_arguments)] *
                 input_arguments.workers)  # and process queue using pool
    print('exit')


def get_univ_files(data_path) -> list:
    return [os.path.join(data_path, x) for x in os.listdir(data_path) if
            os.path.isfile(os.path.join(data_path, x))
            and 'b.html' not in x
            and 'bz.html' not in x
            and 'stat.html' not in x
            and 'index' not in x
            and 'o' not in x]


def get_city_name_from_address(address):
    key = ''
    google_maps_api_url = \
        "https://maps.googleapis.com/maps/api/geocode/json?address=" + \
        address.replace(' ', "+") + \
        "&language=uk&sensor=false&region=uk&key=" + key
    req = requests.get(google_maps_api_url)
    res = req.json()
    try:
        result = res['results'][0]
        for address_component in result['address_components']:
            if ('administrative_area_level_1' in address_component['types']) \
                    and ('political' in address_component['types']):
                # here our city name - however we need to tes
                return address_component['long_name']

        for address_component in result['address_components']:
            if ('administrative_area_level_2' in address_component['types']) \
                    and ('political' in address_component['types']):
                # here our city name - however we need to tes
                return address_component['long_name']
    except Exception as e:
        print(e)
        print(address)
        return ''


def parse_univ_pages_and_write_to_database(input_arguments):
    db = connect_to_database(input_arguments.db_host, input_arguments.db)
    if input_arguments.erase:
        db.univs.drop()
    html_univ_files = get_univ_files(input_arguments.path)
    univs_to_insert = dict()
    for univ_file in html_univ_files:
        with open(univ_file, 'rb') as file:
            q = file.read()
        univ = get_univ_info_from_page_2017(q)

        tlower = univ['univ_title'].lower()
        if 'коледж' in tlower or 'технікум' in tlower or 'училищ' in tlower:
            continue

        univ['univ_title'] = sub(r'\([^)]*\)', '', univ['univ_title'].strip())
        while '  ' in univ['univ_title']:
            univ['univ_title'] = univ['univ_title'].replace('  ', ' ')

        univ['univ_location'] = get_city_name_from_address(
            univ['univ_address'])
        univ['univ_id'] = generate_id(univ['univ_title'])

        univs_to_insert[univ['univ_id']] = univ
        if len(univs_to_insert) % 10 == 0:
            print(len(univs_to_insert))
    db.univs.insert_many(univs_to_insert.values())


def parse_areas_of_study_and_write_to_database(input_arguments):
    db = connect_to_database(input_arguments.db_host, input_arguments.db)
    if input_arguments.erase:
        db.areas.drop()
        db.courses.drop()
    html_univ_files = get_univ_files(input_arguments.path)
    areas = dict()
    for univ_file in html_univ_files:
        with open(univ_file, 'rb') as file:
            q = file.read()
        local_areas = get_area_course_info(q)
        for key, value in local_areas.items():
            if key not in areas:
                areas[key] = list()
            for course in value:
                if course not in areas[key]:
                    areas[key].append(course)
    print(areas)
    areas_dao = [{'area_id_old': generate_id(title), 'area_title': title}
                 for title in areas.keys()]
    courses_dao = list()
    for title, course_list in areas.items():
        curr_courses = [{'area_id_old': generate_id(title), 'course_title': c}
                        for c in course_list]
        courses_dao.extend(curr_courses)
    db.areas.insert_many(areas_dao)
    db.courses.insert_many(courses_dao)


if __name__ == '__main__':
    file_handler = logging.FileHandler('requests.log')
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    with open('errors.txt', 'w+')as f:
        f.write('')
    parser = create_input_argument_parser()
    args = parser.parse_args()
    # universities, area, courses
    parse_univ_pages_and_write_to_database(args)
    parse_areas_of_study_and_write_to_database(args)
    # requests
    start_parsing_pages_and_write_result_to_database(args)
