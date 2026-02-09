import argparse
from typing import Dict

import illumio_pylo as pylo
from illumio_pylo import ExcelHeader
from . import Command
from .utils.report_writer import ReportWriter

command_name = 'rule-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--prefix-objects-with-type', nargs='?', const=True, default=False,
                        help='Prefix objects with their type (e.g. "label:mylabel")')
    parser.add_argument('--object-types-as-section', action='store_true', default=False,
                        help="Consumer and providers will show objects types section headers, example:\n" +
                             "LABELS:\n" +
                             "R-WEB\n" +
                             "A-FUSION\n" +
                             "IPLISTS:\n" +
                             "Private_Networks\n" +
                             "Public_NATed")

    # Add standard report arguments (static helper)
    ReportWriter.add_arguments_to_parser(
        parser,
        default_prefix='rule-export',
        default_sheet_name='rulesets'
    )


def __main(args: Dict, org: pylo.Organization, **kwargs):
    setting_prefix_objects_with_type: bool | str = args['prefix_objects_with_type']
    setting_object_types_as_section: bool = args['prefix_objects_with_type']

    # Initialize report writer will be created after headers are known below

    if setting_prefix_objects_with_type is False:
        print(" * Prefix for object types are disabled")
    else:
        print(" * Prefix for object types are enabled")

    if setting_object_types_as_section is False:
        print(" * Object types as section are disabled")
    else:
        print(" * Object types as section are enabled")

    # Initialize report structure
    csv_report_headers = pylo.ExcelHeaderSet([
        ExcelHeader(name='ruleset', max_width=40),
        ExcelHeader(name='scope', max_width=50),
        ExcelHeader(name='type', max_width=10),
        ExcelHeader(name='consumers', max_width=80),
        ExcelHeader(name='providers', max_width=80),
        ExcelHeader(name='services', max_width=30),
        ExcelHeader(name='options', max_width=40),
        ExcelHeader(name='ruleset_url', max_width=40, wrap_text=False),
        ExcelHeader(name='ruleset_href', max_width=30, wrap_text=False)
    ])

    # Create report writer and its sheet using the header definitions
    report_writer = ReportWriter(headers=csv_report_headers, sheet_name='rulesets', filename_prefix='rule-export')
    report_writer.initialize_from_args(args)
    csv_report = report_writer.excel_workbook
    sheet = report_writer.sheet

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

            consumers_str = rule.consumers.members_to_str("\n",
                                                          prefix_objects_with_type=setting_prefix_objects_with_type,
                                                          object_types_as_section=setting_object_types_as_section)
            providers_str = rule.providers.members_to_str("\n",
                                                          prefix_objects_with_type=setting_prefix_objects_with_type,
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

    # Always write report (even if empty)
    # JSON is now generated from the populated sheet inside ReportWriter
    report_writer.write_reports()


command_object = Command(command_name, __main, fill_parser)
