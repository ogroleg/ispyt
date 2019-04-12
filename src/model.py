from src.exceptions import InvalidRequestParameter

LENGTHS = {
    "regions": [2, 255],
    "univs": [8, 65535],
    "knowledge_areas": [4, 255]
}

PART_TOP_TYPES = ['gov_exams', 'school_score', 'overall']


class Filter:
    keys_types = {
        'univ_ids': list,
        'univ_titles': list,
        'knowledge_areas': list,
        'part_top_applicants': dict,
        'regions': list, 'years': list,
        'enrolled_only': bool}
    keys = set(keys_types.keys())

    def __init__(self, obj: dict):
        extra_fields = set(obj.keys()) - self.keys
        if extra_fields:
            raise InvalidRequestParameter(f'Unknown fields: {extra_fields}')
        for key in obj.keys():
            if type(obj[key]) != self.keys_types[key]:
                raise InvalidRequestParameter(
                    f'Invalid type for field {key}: Actual:{type(obj[key])}, '
                    f'Expected: {self.keys_types[key]}')
        list(map(lambda key: setattr(self, key, obj.get(key, None)),
                 Filter.keys))


class FilterRequest:
    keys = {'filter', 'sort_by'}

    def __init__(self, obj: dict):
        extra_fields = set(obj.keys()) - self.keys
        if extra_fields:
            raise InvalidRequestParameter(f'Unknown fields: {extra_fields}')
        if 'filter' not in obj.keys():
            raise InvalidRequestParameter(f'Field "filter" is required')
        self.filter = Filter(obj['filter'])
        # TODO: sort
