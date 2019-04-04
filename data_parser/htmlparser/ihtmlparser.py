class IHtmlParser(object):
    def __init__(self, row):
        self.row_elements = row.getchildren()
        self.title = self._get_title_of_row(row)
        self.n_columns = len(self.row_elements)

    @staticmethod
    def _get_title_of_row(row):
        if not row.getchildren():
            return ''
        style_value = row.getchildren()[0].attrib['style']
        # if background is white - there is no title to row
        if style_value == 'background:#fff':
            return ''
        return row.attrib['title']

    def get_full_name(self) -> str:
        return self.row_elements[1].text.strip()
