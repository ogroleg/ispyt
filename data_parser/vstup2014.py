import argparse
import multiprocessing
import os
import queue
import threading

import pymongo
from lxml import html

# constants
from data_parser.properties import FILE_ENCODING

MAX_FILE_CACHE_SIZE = 2000  # per process
FILE_CACHE_DELAY = 0.2  # seconds to wait
NUM_RESULTS_TO_SAVE = 25 * 1000
DB_CONNECTION_TIMEOUT = 10 * 1000  # ms


def process_page(file_name: str, file_string: str):
    """
    Parses html file - table with requests and it's head. CPU intensive work.
    :param file_name: str
    :param file_string: str
    :return: list[dict]
    """
    results = []

    univ_id, list_id = map(int, file_name[file_name.rindex('i') + 1:-5].split('p'))

    _ = '<div id=title>'
    header_str = file_string[file_string.index(_) + len(_):]
    header_str = header_str[:header_str.index('</div>')]

    header = html.fragment_fromstring(header_str)
    is_denna = 'денна' in header.getchildren()[-1].tail
    is_zaochna = 'заочна' in header.getchildren()[-1].tail

    assert any((is_denna, is_zaochna)), header.getchildren()[-1].tail

    file_string = file_string[file_string.index('<tbody>') + 7: file_string.rindex('<thead>') - 1]
    requests = html.fragments_fromstring(file_string)

    request: html.HtmlElement
    for request in requests:
        children = request.getchildren()

        full_name = children[1].text.strip().split()
        surname = full_name[0]
        name = ' '.join(full_name[1:2])
        given_name = ' '.join(full_name[2:]).strip()

        olymp_man, prep_courses = 0, 0

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
            'num_recommendations': 0,

            'univ_id': univ_id,
            'list_id': list_id,
            'is_denna': is_denna,
            'is_zaochna': is_zaochna
        }

        results.append(result)

    return results


def save_results_to_db(db, results):
    """
    Saves data after processing to db.
    :param db: active connection to db
    :param results: list
    """
    db.requests.insert(results)


def worker_main(inputs, all_files_are_read):
    """
    Start point of each process from Pool. Reads files, parses them and saves results to db.
    Each process uses separate connection to database.
    """

    db = pymongo.MongoClient(host=args.db_host, connectTimeoutMS=DB_CONNECTION_TIMEOUT)\
        .get_database(args.db)  # connect to db
    results = []


    while not all_files_are_read.is_set() or not inputs.empty():
        try:
            file_name, file_string = inputs.get(timeout=FILE_CACHE_DELAY)  # get file contents from cache
        except queue.Empty:
            continue

        result = process_page(file_name, file_string)  # parse file
        results.extend(result)

        if len(results) > NUM_RESULTS_TO_SAVE:  # and save to db
            save_results_to_db(db, results)
            results = []

    if results:  # ensure everything is saved
        save_results_to_db(db, results)

def read_files_timer(
        inputs: multiprocessing.Queue,
        file_strings: queue.Queue,
        all_files_are_read: threading.Event,
        seconds: float = 0):
    """
    Schedule reading files to cache. Delay equals to seconds parameter.
    """
    t = threading.Timer(seconds, read_files, args=(inputs, file_strings, all_files_are_read))
    t.start()


def read_files(
        inputs: multiprocessing.Queue,
        file_strings: queue.Queue,
        all_files_are_read: threading.Event):
    """
    Read files to cache. Supposed to run in separate thread.
    :param inputs: originally provided filenames
    :param file_strings: queue to put contents of files
    :param all_files_are_read: flag to indicate that no files left
    """
    while not inputs.empty() and file_strings.qsize() < MAX_FILE_CACHE_SIZE:
        try:
            file_path = inputs.get_nowait()  # get path to file
        except queue.Empty:
            break  # error getting path, no files left or we should wait

        path = os.path.join(*file_path)
        with open(path, encoding=FILE_ENCODING) as f:  # and read it
            file_data = f.read()
        # store contents in cache
        file_strings.put((os.path.basename(path), file_data))

    if not inputs.empty():  # wait and repeat
        read_files_timer(inputs, file_strings, all_files_are_read, FILE_CACHE_DELAY)
    else:  # no files left
        print('stop reading')
        all_files_are_read.set()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', dest='db', type=str, help='database name to insert data', required=True)
    parser.add_argument('--erase', dest='erase', action='store_true', help='erases db before inserting data')
    parser.add_argument('--path', dest='path', type=str, help='path to downloaded html files', required=True)
    parser.add_argument('--host', dest='db_host', type=str, help='db host', default='localhost')
    parser.add_argument('--workers', dest='workers', type=int, help='number of workers', default=4)
    parser.set_defaults(erase=False)

    args = parser.parse_args()
    db = pymongo.MongoClient(host=args.db_host, connectTimeoutMS=DB_CONNECTION_TIMEOUT).get_database(args.db)

    if args.erase:
        db.requests.drop()

    prefix_2014 = args.path

    manager = multiprocessing.Manager()
    mp_inputs = manager.Queue()  # queue with inputs

    file_strings = manager.Queue()  # file cache
    all_files_are_read = manager.Event()  # indicator that no files left

    for subdir in os.walk(prefix_2014):
        files = subdir[-1]

        for x in files:
            if 'p' in x:
                mp_inputs.put((subdir[0], x))

    read_files_timer(mp_inputs, file_strings, all_files_are_read)  # start reading in separate thread

    print(f'Total files to process: {mp_inputs.qsize()}')

    pool = multiprocessing.Pool(args.workers)  # create pool
    pool.starmap(worker_main, [(file_strings, all_files_are_read)] * args.workers)  # and process queue using pool

    print('exit')
