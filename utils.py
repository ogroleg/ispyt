import base64


def generate_id(string_code: str):
    return base64.b64encode(string_code.encode('utf-8')).decode('ascii')
