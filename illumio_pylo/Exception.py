from .Helpers import nice_json


class PyloEx(Exception):
    """
    Base class for all Pylo exceptions
    """
    def __init__(self, arg, json_object=None):
        if json_object is None:
            Exception.__init__(self, arg)
            return

        text = "{}\nJSON output:\n{}".format(arg, nice_json(json_object))
        super().__init__(self, text)


class PyloObjectNotFound(PyloEx):
    def __init__(self, arg, json_object=None):
        PyloEx(arg, json_object)


class PyloApiEx(PyloEx):
    """
    Base class for all Pylo API exceptions
    """
    def __init__(self, arg, json_object=None):
        PyloEx(arg, json_object)


class PyloApiTooManyRequestsEx(PyloApiEx):
    def __init__(self, arg, json_object=None):
        PyloApiEx(arg, json_object)


class PyloApiUnexpectedSyntax(PyloApiEx):
    def __init__(self, arg, json_object=None):
        PyloApiEx(arg, json_object)

class PyloApiRequestForbiddenEx(PyloApiEx):
    def __init__(self, arg, json_object=None):
        PyloApiEx(arg, json_object)



