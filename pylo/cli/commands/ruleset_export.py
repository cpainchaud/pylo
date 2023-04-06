import argparse
import os
from typing import Dict

import pylo
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'rule-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--output', required=False, default='.')
    parser.add_argument('--format', required=False, default='excel', choices=['csv', 'excel'])


def __main(options: Dict, org: pylo.Organization, **kwargs):
    csv_report_headers = ['ruleset', 'scope', 'type', 'consumers', 'providers', 'services', 'options',
                          'ruleset_href',
                          ]

    output_file_format = options.get('format')
    if output_file_format == "excel":
        output_file_extension = ".xlsx"
    elif output_file_format == "csv":
        output_file_extension = ".csv"
    else:
        raise Exception("Unknown output file format: %s" % output_file_format)

    output_file_name = options.get('output') + os.sep + make_filename_with_timestamp('rule_export_') + output_file_extension
    output_file_name = os.path.abspath(output_file_name)

    csv_report = pylo.ArrayToExport(csv_report_headers)

    for ruleset in org.RulesetStore.rulesets:
        for rule in ruleset.rules_by_href.values():
            options = []
            if not rule.enabled:
                options.append('disabled')
            if rule.secure_connect:
                options.append('secure-connect')
            if rule.stateless:
                options.append('stateless')
            if rule.machine_auth:
                options.append('machine_auth')

            data = {'ruleset': ruleset.name, 'ruleset_href': ruleset.href, 'scope': ruleset.scopes.get_all_scopes_str(),
                    'consumers': rule.consumers.members_to_str(),
                    'providers': rule.providers.members_to_str(),
                    'services': rule.services.members_to_str(),
                    'options': pylo.string_list_to_text(options)}
            if rule.is_extra_scope():
                data['type'] = 'intra'
            else:
                data['type'] = 'extra'
            csv_report.add_line_from_object(data)

    if output_file_format == "csv":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        csv_report.write_to_csv(output_file_name)
        print("DONE")
    elif output_file_format == "excel":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        csv_report.write_to_excel(output_file_name)
        print("DONE")
    else:
        raise pylo.PyloEx("Unknown format: '{}'".format(options['format']))


command_object = Command(command_name, __main, fill_parser)





