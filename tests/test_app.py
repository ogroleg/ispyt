import logging

import pytest
import requests

from settings import APP_HOST, APP_PORT
from src.logger import configure_logger

logger = logging.getLogger(__file__)
logger = configure_logger(logger, 'tests.log')


@pytest.mark.parametrize('request', [
    {'filter': {
        'knowledge_areas': ['Право'],
        'regions': ['місто Київ'],
        'part_top_applicants': {'type': 'overall', 'value': 20},
        'years': [2017],
        'enrolled_only': True}},
    {'filter': {'knowledge_areas': ['Інформатика та обчислювальна техніка']}},
    {'filter': {'univ_titles': [
        'Київський національний університет імені Тараса Шевченка']}}
])
def test_filter_data(request):
    response = requests.post(f'http://{APP_HOST}:{APP_PORT}/',
                             json=request)
    data = response.json()
    logger.info(data)
    assert data, 'Failed to get the data'
