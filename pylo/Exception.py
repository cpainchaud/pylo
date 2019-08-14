import pylo

class PyloEx(Exception):
    def __init__(self, arg):
        Exception.__init__(self, arg)
