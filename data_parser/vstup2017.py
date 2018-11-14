import multiprocessing
import os
import queue
import threading

from helpers.AdmissionRequest import AbstractAdmissionRequest, AdmissionRequest2017
from helpers.HtmlParser import HtmlParser2017
from helpers.InputArgumentParser import create_input_argument_parser
from helpers.properties import MAX_FILE_CACHE_SIZE, FILE_ENCODING, FILE_CACHE_DELAY, NUM_RESULTS_TO_SAVE
from helpers.db import connect_to_database, save_results_to_db


def get_univ_id_and_list_id_from_filename(file_name: str):
    return list(map(int, file_name[file_name.rindex('i') + 1:-5].split('p')))


def create_request_dictionary_from_raw_data(row, request_with_base_data: AbstractAdmissionRequest):
    processed_request = AdmissionRequest2017(request_with_base_data)
    html_parser = HtmlParser2017(row)
    full_name = html_parser.get_full_name()
    processed_request.set_full_name_and_first_second_last_names(full_name)
    processed_request.extra_points = html_parser.get_extra_points()
    processed_request.rank = html_parser.get_rank()
    processed_request.school_score = html_parser.get_score_of_education_document()
    processed_request.total_score = html_parser.get_total_score()
    processed_request.gov_exams = html_parser.get_government_exam_scores()
    processed_request.priority = html_parser.get_priority()
    processed_request.univ_exams = html_parser.get_university_exams()
    processed_request.coefficients = html_parser.get_coefficients()
    processed_request.is_quota = html_parser.get_is_quota()
    processed_request.is_original = html_parser.get_is_original()
    processed_request.is_enrolled = html_parser.get_is_enrolled()
    return vars(processed_request)


def process_admission_requests(requests: [], base_request: AbstractAdmissionRequest) -> []:
    """
    Convert html rows of admission requests to dictionaries
    :param requests: array of str rows of applicant's admission requests
    :param base_request: request object with common data for all requests
    :return: array of dict of requests
    """
    return [create_request_dictionary_from_raw_data(request, base_request) for request in requests]


def get_common_info_and_create_base_request(file_name: str, file_string: str) -> AbstractAdmissionRequest:
    univ_id, list_id = get_univ_id_and_list_id_from_filename(file_name)
    type_of_education = HtmlParser2017.get_type_of_education(file_string)
    is_denna = 'денна' in type_of_education
    is_zaochna = 'заочна' in type_of_education
    assert any((is_denna, is_zaochna)), type_of_education
    return AbstractAdmissionRequest(univ_id, list_id, is_denna, is_zaochna)


def process_page_with_admission_requests(file_name: str, file_string: str):
    base_request = get_common_info_and_create_base_request(file_name, file_string)
    requests = HtmlParser2017.get_requests_from_page(file_string)
    return process_admission_requests(requests, base_request)


def main_worker(files_cache, is_all_files_read, input_arguments):
    db = connect_to_database(input_arguments.db_host, input_arguments.db)
    results = []
    while not is_all_files_read.is_set() or not files_cache.empty():
        try:
            filename, file_string = files_cache.get(timeout=FILE_CACHE_DELAY)
        except queue.Empty:
            continue
        result = process_page_with_admission_requests(filename, file_string)
        [print(r) for r in result]
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
    while not files_to_read_queue.empty() and file_cache.qsize() < MAX_FILE_CACHE_SIZE:
        try:
            file_path = files_to_read_queue.get_nowait()  # get path to file
        except queue.Empty:
            break  # error getting path, no files left or we should wait
        path = os.path.join(*file_path)
        with open(path, encoding=FILE_ENCODING) as f:  # and read it
            file_data = f.read()
        file_cache.put((os.path.basename(path), file_data))  # store contents in cache


def read_data_from_files(
        files_to_read_queue: multiprocessing.Queue,
        file_cache: multiprocessing.Queue,
        is_all_files_read: multiprocessing.Event):
    read_data_from_files_and_add_to_file_cache(files_to_read_queue, file_cache)
    if not files_to_read_queue.empty():  # wait and repeat
        start_reading_data_with_delay_in_thread(files_to_read_queue, file_cache, is_all_files_read, FILE_CACHE_DELAY)
    else:  # no files left
        print('stop reading')
        is_all_files_read.set()


def start_reading_data_with_delay_in_thread(
        files_to_read_queue: multiprocessing.Queue,
        file_cache: multiprocessing.Queue,
        is_all_files_read: multiprocessing.Event,
        delay: float = 0.0):
    t = threading.Timer(delay, read_data_from_files, args=(files_to_read_queue, file_cache, is_all_files_read))
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
    pool = multiprocessing.Pool(input_arguments.workers)  # create pool
    pool.starmap(main_worker, [
        (file_cache, is_all_files_read, input_arguments)] * input_arguments.workers)  # and process queue using pool
    print('exit')


if __name__ == '__main__':
    parser = create_input_argument_parser()
    args = parser.parse_args()
    start_parsing_pages_and_write_result_to_database(args)
