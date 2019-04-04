import platform

from data_parser.properties import FILE_ENCODING


def to_utf8(string: str) -> str:
    return string.encode(FILE_ENCODING).decode('utf-8') \
        if platform.system().lower() == 'windows' else string
