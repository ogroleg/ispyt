import logging

from lxml import html

from data_parser.common import to_utf8
from data_parser.htmlparser.ihtmlparser import IHtmlParser

logger = logging.getLogger(__name__)


class HtmlParser2017(IHtmlParser):
    OLYMPIAD_OR_MAN_PARTICIPATOR = \
        'Переможець Всеукраїнської олімпіади або конкурсу МАН'
    WORLD_COMPETITION_PARTICIPATOR = 'Учасник міжнародних олімпіад і/або ' \
                                     'призер Олімпійських, Паралімпійських ' \
                                     'і Дефлімпійських ігор'
    EDUCATION_DOCUMENT = 'Середній бал документа про освіту'
    UNIVERSITY_EXAM = 'Фаховий іспит'
    N_COLUMNS_WITH_PRIORITY = 9

    @staticmethod
    def _get_header_from_file_data(file_string: str):
        _ = '<div class="title-page">'
        header_str = file_string[file_string.index(_):]
        header_str = header_str[:header_str.index('</div>') + len('</div>')]
        header_str = header_str.replace('\n', '').replace('\t', '')
        return html.fragment_fromstring(header_str)

    @staticmethod
    def get_course(file_string: str) -> str:
        header = HtmlParser2017._get_header_from_file_data(file_string)
        for item in header.getchildren()[1].getchildren():
            caption = to_utf8(item.tail)
            if caption and 'Спеціальність' in caption:
                return caption.replace('Спеціальність', '') \
                    .replace(':', '').strip()
        return ''

    @staticmethod
    def get_type_of_education(file_string: str) -> str:
        """
        :param file_string: text of html page
        :return: expected to be 'денна' or 'заочна'
        """
        header = HtmlParser2017._get_header_from_file_data(file_string)
        header_items = header.getchildren()[1].getchildren()
        for item in header_items:
            caption = to_utf8(item.tail)
            try:
                if caption is None:
                    continue
                forms = ['дистанційна', 'денна', 'заочна', 'вечірня']
                for form in forms:
                    if form in caption:
                        return caption
            except TypeError:
                logger.error(f'ERROR - Argument of "NoneType" is not iterable:'
                             f' caption={caption}')
        item_texts = list(map(lambda x: x.tail, header_items))
        logger.error(
            f'ERROR - Cannot find the type of education in\n{item_texts}\n')
        return ''

    @staticmethod
    def get_requests_from_page(file_string: str):
        """
        :param file_string: text of html page
        :return: array of html elements with admission request data
        (each element is a row of requests table)
        """
        class_of_table_with_requests = \
            'class="tablesaw tablesaw-stack tablesaw-sortable"'
        start = file_string.index(class_of_table_with_requests)
        file_string = file_string[start + len(class_of_table_with_requests):]
        start = file_string.index('<tbody>')
        end = file_string.index('</table>')
        file_string = to_utf8(file_string[start + len('<tbody>'): end])
        return html.fragments_fromstring(file_string)

    def get_details_element(self):
        i = self.n_columns - 4
        return self.row_elements[i][0]

    def get_coefficients_text(self):
        i = self.n_columns - 3
        return self.row_elements[i].text

    def get_government_exam_scores(self) -> {}:
        """
        Example of government exam details:
        Українська мова та література (ЗНО) 182.00

        Scores of government exams are located in column 'Деталізація'
        :return: {'name_of_exam': score}
        """
        result = {}
        details_scores = self.get_details_element()
        for exam_details in details_scores:
            exam_details = exam_details.text
            if exam_details is not None and 'ЗНО' in exam_details:
                exam_name, score = exam_details.replace('(ЗНО)', '') \
                    .strip().rsplit(' ', 1)
                result[exam_name.strip()] = float(score)
        return result

    def _get_score(self, field: str) -> float:
        details_scores = self.get_details_element()
        for details_score in details_scores:
            details_score = details_score.text
            if details_score is not None and field in details_score:
                score = details_score.replace(field, '').strip()
                return float(score)
        return 0.0

    def get_score_for_olympiad_or_man_participation(self) -> float:
        """
        Example of details if applicant has been a participator in
        olympiad or MAN competition:
        Переможець Всеукраїнської олімпіади або конкурсу МАН 10.0

        :return: 0.0 if field is not found, score value if appropriate
        field is found
        """
        return self._get_score(self.OLYMPIAD_OR_MAN_PARTICIPATOR)

    def get_score_of_education_document(self):
        """
        Example of score of education document details:
        Середній бал документа про освіту 10.40

        :return: 0.0 if field is not found, score value if appropriate
        field is found
        """
        return self._get_score(self.EDUCATION_DOCUMENT)

    def get_university_exams(self):
        """
        Example of score of university exam:
        Творчий конкурс (натура, натюрморт, композиція) 184.00

        :return: {} if field is not found, {exam_name: score} if
        appropriate field is found
        """
        university_exams = {}
        score_exceptions = [self.OLYMPIAD_OR_MAN_PARTICIPATOR,
                            self.WORLD_COMPETITION_PARTICIPATOR,
                            self.EDUCATION_DOCUMENT, 'ЗНО']
        details_scores = self.get_details_element()
        for details_score in details_scores:
            details_score = details_score.text

            if details_score is not None and not any(
                    (exception in details_score
                     for exception in score_exceptions)):
                name, score = details_score.strip().rsplit(' ', 1)
                try:
                    university_exams[name.strip()] = float(score)
                except ValueError as e:
                    logger.error(e)
                    logger.error(f'ERROR - Failed to convert {score} to float')
        return university_exams

    def get_extra_points(self):
        olymp_man = self.get_score_for_olympiad_or_man_participation()
        return {'olymp_man': olymp_man}

    def get_is_original(self) -> bool:
        i = self.n_columns - 1
        return self.row_elements[i].text.strip() == '+'

    def get_rank(self) -> int:
        return int(self.row_elements[0].text)

    def get_total_score(self) -> float:
        i = self.n_columns - 5
        return float(self.row_elements[i].text.strip())

    def get_priority(self) -> int:
        if self.n_columns == self.N_COLUMNS_WITH_PRIORITY:
            priority = self.row_elements[3].text.strip()
            return 0 if '—' in priority else int(priority)
        return 0

    def get_coefficient(self, coefficient: str) -> float:
        """
        Get coefficient values from row
        :param coefficient: type of coefficient
        :return: 0.0 if coefficient not found or equals -, float value
        alternatively
        """
        coefficient_texts = self.get_coefficients_text().split('\n')
        for coefficient_text in coefficient_texts:
            if coefficient in coefficient_text:
                coefficient = coefficient_text.replace(coefficient + ":", '')
                if '—' not in coefficient:
                    return float(coefficient)
                else:
                    break
        return 0.0

    def get_region_coefficient(self) -> float:
        return self.get_coefficient('РK')

    def get_village_coefficient(self) -> float:
        return self.get_coefficient('СK')

    def get_area_coefficient(self) -> float:
        return self.get_coefficient('ГK')

    def get_firstinqueue_coefficient(self) -> float:
        return self.get_coefficient('ПК')

    def get_coefficients(self) -> {}:
        return {
            'РК': self.get_region_coefficient(),
            'СК': self.get_village_coefficient(),
            'ГК': self.get_area_coefficient(),
            'ПК': self.get_firstinqueue_coefficient()
        }

    def get_is_quota(self) -> int:
        i = self.n_columns - 2
        quota = self.row_elements[i].text
        return False if '—' in quota else True

    def get_is_enrolled(self):
        return 'Зараховано' in self.title
