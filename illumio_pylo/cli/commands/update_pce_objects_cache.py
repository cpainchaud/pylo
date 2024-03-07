import logging
import argparse
import illumio_pylo as pylo
import os
import datetime
import json

from illumio_pylo import log
from . import Command


command_name = "pce-objects-cache-updater"
objects_load_filter = None


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--include-deleted-workloads', action='store_true',
                        help='should deleted workloads be downloaded as well')


def __main(args, org: pylo.Organization = None, connector: pylo.APIConnector = None, config_data=None, **kwargs):

    # filename should be like 'cache_xxx.yyy.zzz.json'
    filename = 'cache_' + connector.name + '.json'

    timestamp = datetime.datetime.now(datetime.timezone.utc)

    json_content = {'generation_date': timestamp.isoformat(),
                    'pce_version': connector.get_software_version_string(),
                    'data': config_data,
                    }

    with open(filename, 'w') as outfile:
        json.dump(json_content, outfile)

    size = os.path.getsize(filename)

    print("\nPCE objects and settings were saved to file '%s' with a size of %iKB" % (filename, int(size/1024)))

    print()


command_object = Command(command_name, __main, fill_parser, skip_pce_config_loading=True)

