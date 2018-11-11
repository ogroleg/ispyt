import multiprocessing
import os
import queue
import threading

from lxml import html

from data_parser.admission_request import AbstractAdmissionRequest, AdmissionRequest2017
from data_parser.db import connect_to_database
from data_parser.parser import create_argument_parser
from data_parser.properties import MAX_FILE_CACHE_SIZE, FILE_ENCODING, FILE_CACHE_DELAY, NUM_RESULTS_TO_SAVE
from data_parser.vstup2014 import save_results_to_db


def get_univ_id_and_list_id_from_filename(file_name: str):
    return map(int, file_name[file_name.rindex('i') + 1:-5].split('p'))


def get_header_from_file_data(file_string: str):
    _ = '<div class="title-page">'
    header_str = file_string[file_string.index(_):]
    header_str = header_str[:header_str.index('</div>') + len('</div>')]
    header_str = header_str.replace('\n', '').replace('\t', '')
    return html.fragment_fromstring(header_str)


def get_type_of_education(header):
    return header.getchildren()[1].getchildren()[2].tail.encode('windows-1251').decode('utf-8')


def get_requests_from_body(file_string):
    file_string = file_string[file_string.index('<tbody>') + 7: file_string.rindex('<thead>') - 1]
    return html.fragments_fromstring(file_string)


def get_surname_name_given_name_from_full_name(full_name):
    names = full_name.split()
    return names[0], ' '.join(full_name[1:2]), ' '.join(full_name[2:]).strip()


def create_request_dictionary_from_raw_data(request, request_with_base_data: AbstractAdmissionRequest):
    children = request.getchildren()
    processed_request = AdmissionRequest2017(request_with_base_data)
    full_name = children[1].text.strip()
    surname, name, given_name = get_surname_name_given_name_from_full_name(full_name)
    olymp_man, prep_courses = 0, 0

    # TODO: refactor here
    # this part of code retrieved data from table's raw and pass data to dictionary
    if children[6].text and children[6].text.count('/') == 1:
        temp = children[6].text.strip().split('/')

        try:
            olymp_man, prep_courses = float(temp[0]), float(temp[1])
        except ValueError:
            pass

    extra_points = {'olymp_man': olymp_man, 'prep': prep_courses}

    is_original = children[10].text.strip().split('/')
    result = {
        'rank': int(children[0].text),
        'full_name': children[1].text.strip(),
        'surname': surname,
        'name': name,
        'given_name': given_name,
        'total_score': float(children[2].text),
        'school_score': float(children[3].text),
        'gov_exams': [x.text.strip().split(':') for x in children[4].getchildren() if x.text],
        'univ_exams': [x.text.strip().split(':') for x in children[5].getchildren() if x.text],
        'extra_points': extra_points,
        'is_out_of_competition': children[7].text.strip() == '+',
        'is_prioritized': children[8].text.strip() == '+',
        'is_directed': children[9].text.strip() == '+',
        'is_original': is_original[0] == '+',
        'is_enrolled': 'style' in children[0].attrib,
        'num_applications': 0,
        'num_recommendations': 0
    }
    """
            'univ_id': univ_id,
            'list_id': list_id,
            'is_denna': is_denna,
            'is_zaochna': is_zaochna
    """
    return result


def process_admission_requests(requests):
    return list(map(create_request_dictionary_from_raw_data, requests))


def process_page_with_admission_requests(file_name: str, file_string: str):
    univ_id, list_id = get_univ_id_and_list_id_from_filename(file_name)
    header = get_header_from_file_data(file_string)
    type_of_education = get_type_of_education(header)
    is_denna = 'денна' in type_of_education
    is_zaochna = 'заочна' in type_of_education
    assert any((is_denna, is_zaochna)), type_of_education
    requests = get_requests_from_body(file_string)
    return process_admission_requests(requests)


def main_worker(files_cache, is_all_files_read):
    db = connect_to_database(args.db_host, args.db)
    results = []
    while not is_all_files_read.is_set() or not files_cache.empty():
        try:
            filename, file_string = files_cache.get(timeout=FILE_CACHE_DELAY)
        except queue.Empty:
            continue
        result = process_page_with_admission_requests(filename, file_string)
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


def start_parsing_pages_and_write_result_to_database():
    # db = connect_to_database(args.db_host, args.db)
    # if args.erase:
    #    db.requests.drop()
    path_to_data = args.path
    files_to_read_queue, file_cache, is_all_files_read = create_queues()
    add_files_to_queue(path_to_data, files_to_read_queue)
    read_data_from_files(files_to_read_queue, file_cache, is_all_files_read)
    print(f'Total files to process: {files_to_read_queue.qsize()}')
    pool = multiprocessing.Pool(args.workers)  # create pool
    pool.starmap(main_worker, [(file_cache, is_all_files_read)] * args.workers)  # and process queue using pool
    main_worker(file_cache, is_all_files_read)
    print('exit')


if __name__ == '__main__':
    parser = create_argument_parser()
    args = parser.parse_args()
    start_parsing_pages_and_write_result_to_database()
