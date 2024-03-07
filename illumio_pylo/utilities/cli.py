# this is here only for the convenience of building binaries

import os
import sys

# in case user wants to run this utility while having a version of pylo already installed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
import illumio_pylo.cli

illumio_pylo.cli.run()
