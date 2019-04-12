import logging

import pytest

from db.db import DBPool
from src.logger import configure_logger
from src.model import Filter

logger = logging.getLogger(__file__)
logger = configure_logger(logger, 'tests.log')


def test_get_regions(database: DBPool):
    expected_regions = [
        'місто Київ', 'Львівська область', 'Донецька область',
        'Дніпропетровська область', 'Хмельницька область',
        'Харківська область', 'Yerevan', 'Миколаївська область',
        'Тернопільська область', 'Чернівецька область', 'Вінницька область',
        'Запорізька область', 'Чернігівська область', 'Одеська область',
        'Волинська область', 'Київська область', 'Криворізька область',
        'Сумська область', 'Луганська область', 'Івано-Франківська область',
        'Полтавська область', 'Рівненська область',
        'Закарпатська область', 'Житомирська область', 'Черкаська область',
        'Херсонська область', 'Кіровоградська область']
    db = database
    regions = db.get_regions()
    logger.info(regions)
    assert sorted(expected_regions) == sorted(regions), \
        'Regions are not the same. See logs for details'


def test_get_knowledge_areas(database: DBPool):
    db = database
    areas = db.get_knowledge_areas()
    logger.info(areas)
    assert areas, 'Failed to get ares from the database'


def test_get_university_titles(database: DBPool):
    db = database
    universities = db.get_university_titles()
    logger.info(universities)
    assert universities, 'Failed to get university titles'


@pytest.mark.parametrize('request', [
    {
        'knowledge_areas': ['Право'],
        'regions': ['місто Київ'],
        'part_top_applicants': {'type': 'overall', 'value': 20},
        'years': [2017],
        'enrolled_only': True}])
def test_get_requests_by_filter(database: DBPool, request: dict):
    """
    Gets Requests from database by filter
    :param database:
    :param request:
    :return:
    """
    filter_request = Filter(request)
    logger.debug(filter_request)
    requests = database.get_requests_by_filter(filter_request)
    logger.info(requests)
    assert requests, 'Failed to filter requests'
    univs = database.get_universities_by_requests(requests)
    logger.info(univs)
    assert univs, 'Failed to get universities'
    univ_ids = [univ['univ_id'] for univ in univs]
    labels_fields = {
        'average_overall_score': 'total_score',
        'average_school_score': 'school_score'
    }
    additional_info = database.get_additional_data_by_univ(
        univ_ids, labels_fields)
    assert len(additional_info) == len(univ_ids)
    logger.info(additional_info)
    assert additional_info, 'Failed to get additional info'
    for info, univ in zip(additional_info, univs):
        info.update(univ)
        del info['_id']
    logger.info(additional_info)


@pytest.mark.parametrize('title', [
    'Національний університет "Києво-Могилянська академія"'])
def test_get_knowledge_areas_by_universities(database: DBPool, title: str):
    knowledge_areas = database.get_knowledge_areas_by_university(title)
    logger.info(f'{title} ==> knowledge_areas {knowledge_areas}')
    assert knowledge_areas, 'Knowledge areas has not been found'
