from .Exception import PyloEx
from .LabelStore import label_type_role, label_type_env, label_type_loc, label_type_app


class LabelCommon:

    def __init__(self, name: str, href: str, ltype: int, owner):
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
                ltype = label_type_loc
            elif ltype == 'env':
                ltype = label_type_env
            elif ltype == 'app':
                ltype = label_type_app
            elif ltype == 'role':
                ltype = label_type_role
            else:
                raise PyloEx("Tried to initialize a Label object with unsupported type '%s'" % (ltype) )

        self._type = ltype

    def is_label(self):
        raise PyloEx("not implemented")

    def is_group(self):
        raise PyloEx("not implemented")

    def type_to_short_string(self):
        if self.type_is_location():
            return "loc"
        elif self.type_is_environment():
            return "env"
        elif self.type_is_application():
            return "app"
        elif self.type_is_role():
            return "role"

        raise PyloEx("unsupported yet")


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

    def type_string(self) -> str:
        if self._type == label_type_loc:
            return 'loc'
        if self._type == label_type_env:
            return 'env'
        if self._type == label_type_app:
            return 'app'
        if self._type == label_type_role:
            return 'role'
        raise PyloEx("unsupported Label type #{} for label href={}".format(self._type, self.href))

