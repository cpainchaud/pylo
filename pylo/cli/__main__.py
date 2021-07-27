import sys
import os

if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from pylo.cli import run

if __name__ == "__main__":
    run()
