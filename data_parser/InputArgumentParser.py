import argparse


def create_input_argument_parser() -> argparse.ArgumentParser:
    """
    :return: parser of input arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', dest='db', type=str,
                        help='database name to insert data', required=True)
    parser.add_argument('--erase', dest='erase', action='store_true',
                        help='erases db before inserting data')
    parser.add_argument('--path', dest='path', type=str,
                        help='path to downloaded html files', required=True)
    parser.add_argument('--host', dest='db_host', type=str,
                        help='db host', default='localhost')
    parser.add_argument('--workers', dest='workers', type=int,
                        help='number of workers', default=4)
    parser.set_defaults(erase=False)
    return parser
