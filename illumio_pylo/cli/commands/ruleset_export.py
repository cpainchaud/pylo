import argparse
import os
from typing import Dict, List, Literal

import illumio_pylo as pylo
from illumio_pylo import ArraysToExcel, ExcelHeader
from .utils.misc import make_filename_with_timestamp
from . import Command

command_name = 'rule-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--format', '-f', required=False, default='excel',choices=['csv', 'excel'], help='Output file format')
    parser.add_argument('--output-dir', '-o', required=False, default='output', help='Directory where to save the output file')
    parser.add_argument('--prefix-objects-with-type', nargs='?', const=True, default=False,
                        help='Prefix objects with their type (e.g. "label:mylabel")')
    parser.add_argument('--object-types-as-section', action='store_true', default=False,
                        help="Consumer and providers will show objects types section headers, example:" + os.linesep +
                        "LABELS: " + os.linesep +
                        "R-WEB" + os.linesep +
                        "A-FUSION" + os.linesep +
                        "IPLISTS: " + os.linesep +
                        "Private_Networks" + os.linesep +
                        "Public_NATed")



def __main(args: Dict, org: pylo.Organization, **kwargs):

    setting_prefix_objects_with_type: bool|str = args['prefix_objects_with_type']
    setting_object_types_as_section: bool = args['prefix_objects_with_type']
    settings_output_file_format = args['format']
    settings_output_dir = args['output_dir']


    if setting_prefix_objects_with_type is False:
        print(" * Prefix for object types are disabled")
    else:
        print(" * Prefix for object types are enabled")


    if setting_object_types_as_section is False:
        print(" * Object types as section are disabled")
    else:
        print(" * Object types as section are enabled")

    csv_report, output_file_name, sheet = prepare_csv_report_object(settings_output_file_format, settings_output_dir)

    for ruleset in org.RulesetStore.rulesets:
        for rule in ruleset.rules_ordered_by_type:
            rule_options = []
            if not rule.enabled:
                rule_options.append('disabled')
            if rule.secure_connect:
                rule_options.append('secure-connect')
            if rule.stateless:
                rule_options.append('stateless')
            if rule.machine_auth:
                rule_options.append('machine_auth')

            scope_str = ''
            for scope in ruleset.scopes.scope_entries.values():
                if len(scope_str) > 0:
                    scope_str += "\n"
                if scope.is_all_all_all():
                    scope_str += "*ALL LABELS*"
                    continue
                for label in scope.labels_sorted_by_type:
                    scope_str += f"{label.name}\n"
            # remove last \n from scope
            if scope_str[-1] == "\n":
                scope_str = scope_str[:-1]


            consumers_str = rule.consumers.members_to_str("\n", prefix_objects_with_type=setting_prefix_objects_with_type,
                                                            object_types_as_section=setting_object_types_as_section)
            providers_str = rule.providers.members_to_str("\n", prefix_objects_with_type=setting_prefix_objects_with_type,
                                                          object_types_as_section=setting_object_types_as_section)


            data = {'ruleset': ruleset.name, 'scope': scope_str,
                    'consumers': consumers_str,
                    'providers': providers_str,
                    'services': rule.services.members_to_str("\n"),
                    'options': pylo.string_list_to_text(rule_options, "\n"),
                    'ruleset_href': ruleset.href,
                    'ruleset_url': ruleset.get_ruleset_url(),
                    'type': 'intra' if rule.is_intra_scope() else 'extra'
                    }

            sheet.add_line_from_object(data)

    if settings_output_file_format == "csv":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        sheet.write_to_csv(output_file_name)
        print("DONE")
    elif settings_output_file_format == "excel":
        print(" * Writing export file '{}' ... ".format(output_file_name), end='', flush=True)
        csv_report.write_to_excel(output_file_name)
        print("DONE")
    else:
        raise pylo.PyloEx("Unknown format: '{}'".format(args['format']))


def prepare_csv_report_object(output_file_format: Literal['excel', 'csv'], settings_output_dir: str):
    if output_file_format == "excel":
        output_file_extension = ".xlsx"
    elif output_file_format == "csv":
        output_file_extension = ".csv"
    else:
        raise Exception("Unknown output file format: %s" % output_file_format)
    csv_report_headers = pylo.ExcelHeaderSet(
        [ExcelHeader(name='ruleset', max_width=40),
         ExcelHeader(name='scope', max_width=50),
         ExcelHeader(name='type', max_width=10),
         ExcelHeader(name='consumers', max_width=80),
         ExcelHeader(name='providers', max_width=80),
         ExcelHeader(name='services', max_width=30),
         ExcelHeader(name='options', max_width=40),
         ExcelHeader(name='ruleset_url', max_width=40, wrap_text=False),
         ExcelHeader(name='ruleset_href', max_width=30, wrap_text=False)
         ])
    csv_report = ArraysToExcel()
    sheet = csv_report.create_sheet('rulesets', csv_report_headers, force_all_wrap_text=True,
                                    multivalues_cell_delimiter=',')
    output_file_name = make_filename_with_timestamp('rule_export_', settings_output_dir) + output_file_extension
    return csv_report, output_file_name, sheet


command_object = Command(command_name, __main, fill_parser)





