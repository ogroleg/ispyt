import base64
import os
import sys

WINDOWS = (sys.platform.startswith("win") or
           (sys.platform == 'cli' and os.name == 'nt'))


def generate_id(string_code: str):
    return base64.b64encode(string_code.encode('utf-8')).decode('ascii')


def decode_id(generated_code: str):
    try:
        return base64.b64decode(generated_code.encode('ascii')).decode('utf-8')
    except Exception as e:
        print(generated_code)
        raise RuntimeError(e)
