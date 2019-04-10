class AbstractAdmissionRequest(object):
    def __init__(self, univ_id: int, list_id: int,
                 is_denna: bool, is_zaochna: bool, course_id: str):
        self.univ_id = univ_id
        self.course_id = course_id
        self.list_id = list_id
        self.is_denna = is_denna
        self.is_zaochna = is_zaochna
        self.full_name = ''
        self.first_name = ''
        self.middle_name = ''
        self.last_name = ''
        self.total_score = 0.0
        self.school_score = 0.0
        self.gov_exams = {}
        self.univ_exams = {}
        self.extra_points = 0.0
        self.is_original = False
        self.is_enrolled = False
        self.rank = 0


class AdmissionRequest2014(AbstractAdmissionRequest):
    def __init__(self, base: AbstractAdmissionRequest):
        super(AdmissionRequest2014, self).__init__(
            base.univ_id,
            base.list_id,
            base.is_denna,
            base.is_zaochna
        )
        self.is_out_of_competition = False
        self.is_prioritized = False
        self.is_directed = False
        self.num_applications = 0
        self.num_recommendations = 0


class AdmissionRequest2017(AbstractAdmissionRequest):
    def __init__(self, base: AbstractAdmissionRequest):
        super(AdmissionRequest2017, self).__init__(
            base.univ_id,
            base.list_id,
            base.is_denna,
            base.is_zaochna,
            base.course_id
        )
        self.priority = 0
        self.coefficients = {}
        self.is_quota = 0

    def set_full_name_and_first_second_last_names(self, full_name):
        self.full_name = full_name
        names = full_name.split(' ')
        self.last_name = names[0]
        self.first_name = ' '.join(names[1:2])
        self.middle_name = ' '.join(names[2:]).strip()
