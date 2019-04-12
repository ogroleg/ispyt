from bs4 import BeautifulSoup


# noinspection PyPep8Naming
class UNIV_FIELDS:
    HEI_NAME = 'Назва ВНЗ:'
    HEI_TYPE = 'Тип ВНЗ:'
    ADDRESS = 'Адреса:'


def is_university_state_owned(title: str):
    return 'національн' in title.lower()


def get_univ_info_from_page_2017(univ_page):
    def get_children(tag):
        tag_children = list(tag.children)
        if not tag_children:
            return '', ''
        if len(list(tag_children)) == 1 and ':' in tag_children[0].text:
            tags = tag_children[0].text.split(':')
            return [tags[0] + ':', tags[1].strip()]
        return [t.text.strip() for t in tag_children]

    soup = BeautifulSoup(univ_page, 'html.parser')
    all_table_rows = soup.find_all('tr')
    info_table = list(map(get_children, all_table_rows[:9]))
    candidates = {key: value for key, value in info_table}
    univ_title = candidates.get(UNIV_FIELDS.HEI_NAME, '')
    is_state_owned = is_university_state_owned(univ_title)
    univ_info = {
        'univ_title': univ_title,
        'univ_type': candidates.get(UNIV_FIELDS.HEI_TYPE, ''),
        'univ_address': candidates.get(UNIV_FIELDS.ADDRESS, ''),
        'is_state_owned': is_state_owned
    }
    return univ_info


def find_area_and_course(current_values):
    area = ''
    course = ''
    for value in current_values:
        if isinstance(value, str):
            continue
        if len(list(value.children)) > 1:
            new_area, new_course = find_area_and_course(value.children)
            if new_area and new_course:
                return new_area, new_course
            if new_area:
                area = new_area
            if new_course:
                course = new_course
        if value.attrs.get('title', '') == 'Галузь':
            area = value.text.strip()
        if value.attrs.get('title', '') == 'Спеціальність':
            course = value.text.strip()
    return area, course


def get_area_course_info(univ_page):
    def get_children(tag):
        tag_children = list(tag.children)
        if not tag_children:
            return None
        if len(tag_children) >= 9:
            return None
        area, course = find_area_and_course(tag_children[0].children)
        if area and course:
            return area, course

    soup = BeautifulSoup(univ_page, 'html.parser')
    all_table_rows = soup.find_all('tr')
    area_courses = dict()
    course_tables = list(filter(None, map(get_children, all_table_rows[9:])))
    for key, value in course_tables:
        if key not in area_courses:
            area_courses[key] = list()
        if value not in area_courses[key]:
            area_courses[key].append(value)
    return area_courses
