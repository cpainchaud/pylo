import pylo


class PyloEx(Exception):
    def __init__(self, arg, json_object=None):
        if json_object is None:
            Exception.__init__(self, arg)
            return

        text = "{}\nJSON output:\n{}".format(arg, pylo.nice_json(json_object))
        Exception.__init__(self, text)

