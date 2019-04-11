import base64
import os
import sys

WINDOWS = (sys.platform.startswith("win") or
           (sys.platform == 'cli' and os.name == 'nt'))


def generate_id(string_code: str):
    return base64.b64encode(string_code.encode('utf-8')).decode('ascii')
