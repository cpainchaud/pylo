import argparse
import os
from typing import Dict, List

import pylo
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'rule-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--output', required=False, default='.')
    parser.add_argument('--format', required=False, default='excel', choices=['csv', 'excel'])


def __main(options: Dict, org: pylo.Organization, **kwargs):
    csv_report_headers: List[pylo.ExcelHeader|str] = [{'name':'ruleset', 'max_width': 40},
                                                      {'name':'scope', 'max_width': 50},
                                                      {'name':'type', 'max_width': 10},
                                                      {'name':'consumers', 'max_width': 80},
                                                      {'name':'providers', 'max_width': 80},
                                                      {'name':'services', 'max_width': 30},
                                                      {'name':'options', 'max_width': 40},
                                                      {'name':'ruleset_url', 'max_width': 40, 'wrap_text': False},
                                                      {'name':'ruleset_href', 'max_width': 30, 'wrap_text': False}
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

    csv_report = pylo.ArraysToExcel()
    sheet = csv_report.create_sheet('rulesets', csv_report_headers, force_all_wrap_text=True, multivalues_cell_delimiter=',')

    for ruleset in org.RulesetStore.rulesets:
        for rule in ruleset.rules_by_href.values():
            rule_options = []
            if not rule.enabled:
                rule_options.append('disabled')
            if rule.secure_connect:
                rule_options.append('secure-connect')
            if rule.stateless:
                rule_options.append('stateless')
            if rule.machine_auth:
                rule_options.append('machine_auth')

            data = {'ruleset': ruleset.name, 'ruleset_href': ruleset.href, 'scope': ruleset.scopes.get_all_scopes_str(),
                    'consumers': rule.consumers.members_to_str("\n"),
                    'providers': rule.providers.members_to_str("\n"),
                    'services': rule.services.members_to_str("\n"),
                    'options': pylo.string_list_to_text(rule_options, "\n"),
                    'ruleset_url': ruleset.get_ruleset_url()}
            if rule.is_extra_scope():
                data['type'] = 'intra'
            else:
                data['type'] = 'extra'
            sheet.add_line_from_object(data)

    if output_file_format == "csv":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        sheet.write_to_csv(output_file_name)
        print("DONE")
    elif output_file_format == "excel":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        csv_report.write_to_excel(output_file_name)
        print("DONE")
    else:
        raise pylo.PyloEx("Unknown format: '{}'".format(options['format']))


command_object = Command(command_name, __main, fill_parser)





