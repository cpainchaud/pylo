import pylo


class LabelGroup(pylo.ReferenceTracker, pylo.LabelCommon):

    """
    :type _members: dict[str,pylo.Label|pylo.LabelGroup]
    """

    def __init__(self, name, href, ltype, owner):
        pylo.ReferenceTracker.__init__(self)
        pylo.LabelCommon.__init__(self, name, href, ltype, owner)
        self._members = {}
        self.raw_json = None

    def load_from_json(self):
        # print(self.raw_json)
        if 'sub_groups' in self.raw_json:
            for href_record in self.raw_json['sub_groups']:
                if 'href' in href_record:
                    find_label = self.owner.find_by_href_or_die(href_record['href'])
                    find_label.add_reference(self)
                    self._members[find_label.name] = find_label
                else:
                    raise pylo.PyloEx('LabelGroup member has no HREF')



    def is_group(self):
        return True

    def is_label(self):
        return False

