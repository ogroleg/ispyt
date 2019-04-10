import logging

import pytest

from db.db import DBPool

logger = logging.getLogger(__name__)
logger.addHandler(logging.FileHandler('tests.log'))


@pytest.fixture
def database():
    db = DBPool('localhost', 27017)
    db.connect('ispyt')
    yield db


def test_get_regions(database: DBPool):
    expected_regions = [
        'місто Київ', 'Львівська область', 'Донецька область',
        'Дніпропетровська область', 'Хмельницька область',
        'Харківська область',
        'Тернопільська область', 'Чернівецька область', 'Вінницька область',
        'Запорізька область', 'Чернігівська область', 'Одеська область',
        'Волинська область', 'Київська область', 'Запорізька область',
        'Сумська область', 'Луганська область', 'Івано-Франківська область',
        'Полтавська область', 'Миколаївська область', 'Рівненська область',
        'Закарпатська область', 'Житомирська область', 'Черкаська область',
        'Херсонська область', 'Кіровоградська область', 'Рівненська область',
        'Дніпропетровська', 'Києво-Святошинський район', 'Миколаївська область'
    ]
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
    {'area_title': ['Охорона здоров’я', 'Право']}
])
def test_get_regions_by_filter(database: DBPool, request: dict):
    regions = database.get_regions_by_filter(request)
    logger.info(regions)
    assert regions, f'Failed to filter regions by request={request}'
