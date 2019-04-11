from dataclasses import dataclass

from dataclasses_json import dataclass_json

LENGTHS = {
    "regions": [2, 255],
    "univs": [8, 65535],
    "knowledge_areas": [4, 255]
}

PART_TOP_TYPES = ['gov_exams', 'school_score']


@dataclass_json
@dataclass
class UnivInfo:
    univ_title: str
    univ_id: str


@dataclass_json
@dataclass
class SetupResponse:
    tags: dict
    knowledge_areas: dict
    regions: dict
    univs: list


@dataclass_json
@dataclass
class Filter:
    univ_ids: list
    knowledge_regions: list
    part_top_applicants: dict
    regions: list
    years: list
    is_enrolled: bool


@dataclass_json
@dataclass
class FilterRequest:
    filter: Filter
