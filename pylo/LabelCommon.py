import pylo
from .LabelStore import label_type_role, label_type_env, label_type_loc, label_type_app


class LabelCommon:

    owner = ...  # type: pylo.LabelStore
    name = ...  # type: str
    href = ...  # type: str

    def __init__(self, name, href, ltype, owner):
        """
        :param name: str
        :param href: str
        :param ltype: int|str
        """
        self.owner = owner
        self.name = name
        self.href = href

        if type(ltype) is str:
            if ltype == 'loc':
                ltype = 1
            elif ltype == 'env':
                ltype = 2
            elif ltype == 'app':
                ltype = 4
            elif ltype == 'role':
                ltype = 3
            else:
                raise Exception("Tried to initialize a Label object with unsupported type '%s'" % (ltype) )

        self._type = ltype

    def is_label(self):
        raise Exception("not implemented")

    def is_group(self):
        raise Exception("not implemented")

    def type_to_short_string(self):
        if self.type_is_location():
            return "loc"
        elif self.type_is_environment():
            return "env"
        elif self.type_is_application():
            return "app"
        elif self.type_is_role():
            return "role"

        raise Exception("unsupported yet")


    def type_is_location(self):
        return self._type == label_type_loc

    def type_is_environment(self):
        return self._type == label_type_env

    def type_is_application(self):
        return self._type == label_type_app

    def type_is_role(self):
        return self._type == label_type_role

    def type(self):
        return self._type

    def type_string(self):
        if self._type == label_type_loc:
            return 'loc'
        if self._type == label_type_env:
            return 'env'
        if self._type == label_type_app:
            return 'app'
        if self._type == label_type_role:
            return 'role'

