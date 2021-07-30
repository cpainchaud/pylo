import pylo
import argparse
from .misc import make_filename_with_timestamp
from . import Command

command_name = 'rule-export'


def fill_parser(parser: argparse.ArgumentParser):
    parser.add_argument('--output', required=False, default='.')


def __main(options, org: pylo.Organization, **kwargs):

    output_file_prefix = make_filename_with_timestamp('rule_export_')
    output_file_csv = output_file_prefix + '.csv'
    output_file_excel = output_file_prefix + '.xlsx'

    csv_report_headers = ['ruleset', 'scope', 'type', 'consumers', 'providers', 'services', 'options',
                          'ruleset_href', 'rule_href'
                          ]

    csv_report = pylo.ArrayToExport(csv_report_headers)

    for ruleset in org.RulesetStore.itemsByHRef.values():
        for rule in ruleset.rules_byHref.values():
            data = {'ruleset': ruleset.name, 'ruleset_href': ruleset.href, 'scope': ruleset.scopes.get_all_scopes_str(),
                    'consumers': rule.consumers.members_to_str(),
                    'providers': rule.providers.members_to_str(),
                    'services': rule.services.members_to_str(),
                    'rule_href': rule.href}
            if rule.is_extra_scope():
                data['type'] = 'intra'
            else:
                data['type'] = 'extra'
            csv_report.add_line_from_object(data)

    print(" * Writing export file '{}' ... ".format(output_file_csv), end='', flush=True)
    csv_report.write_to_csv(output_file_csv)
    print("DONE")
    print(" * Writing export file '{}' ... ".format(output_file_excel), end='', flush=True)
    csv_report.write_to_excel(output_file_excel)
    print("DONE")


command_object = Command(command_name, __main, fill_parser)





