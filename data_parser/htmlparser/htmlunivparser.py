from bs4 import BeautifulSoup


# noinspection PyPep8Naming
class UNIV_FIELDS:
    HEI_NAME = 'Назва ВНЗ:'
    HEI_TYPE = 'Тип ВНЗ:'
    ADDRESS = 'Адреса:'


def is_university_state_owned(title: str):
    return 'національний' in title.lower() or 'національна' in title.lower()


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
    info_table = list(map(get_children, soup.find_all('tr')[:9]))
    candidates = {key: value for key, value in info_table}
    univ_title = candidates.get(UNIV_FIELDS.HEI_NAME, '')
    is_state_owned = is_university_state_owned(univ_title)
    return {
        'univ_title': univ_title,
        'univ_type': candidates.get(UNIV_FIELDS.HEI_TYPE, ''),
        'univ_address': candidates.get(UNIV_FIELDS.ADDRESS, ''),
        'is_state_owned': is_state_owned
    }
