from datetime import datetime
import pylo


def make_filename_with_timestamp(prefix: str):
    now = datetime.now()
    return prefix + now.strftime("%Y%m%d-%H%M%S")