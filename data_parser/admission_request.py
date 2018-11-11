class AbstractAdmissionRequest(object):
    def __init__(self, univ_id, list_id, is_denna, is_zaochna):
        self.univ_id = univ_id,
        self.list_id = list_id,
        self.is_denna = is_denna,
        self.is_zaochna = is_zaochna
        # TODO: describe all fields of admission request


class AdmissionRequest2017(AbstractAdmissionRequest):
    def __init__(self, base: AbstractAdmissionRequest):
        super(AdmissionRequest2017, self).__init__(
            base.univ_id,
            base.list_id,
            base.is_denna,
            base.is_zaochna
        )
