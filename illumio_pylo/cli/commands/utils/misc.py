import os
from datetime import datetime
import illumio_pylo as pylo


def make_filename_with_timestamp(prefix: str, output_directory: str = './') -> str:
    # if output directory starts with a dot, we assume it's a relative path. Same if not it's not starting with a slash
    if output_directory.startswith('.') or not output_directory.startswith('/') or not output_directory.startswith('\\'):
        output_directory = os.path.realpath(os.getcwd() + os.path.sep + output_directory)

    # if the directory does not exist, we will create it and its parents if needed
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    now = datetime.now()
    return output_directory + os.path.sep + prefix + now.strftime("%Y%m%d-%H%M%S")