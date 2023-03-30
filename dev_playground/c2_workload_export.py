from typing import Dict

import pylo
import sys
import os
from pylo.cli import run
from pylo.cli.commands.workload_export import ExtraColumn
from pylo import Organization
from pylo import string_list_to_text
import argparse


class LabelGroupOwnershipColumn (ExtraColumn):
    def __init__(self):
        super().__init__()

    def column_description(self):
        return self.ColumnDescription('label_group_ownership', 'Label Group Ownership')

    def apply_cli_args(self, parser: argparse.ArgumentParser):
        parser.add_argument('--filter-label-groups', type=str, required=False, default=None,
                            help='CSV field delimiter')
        pass

    def post_process_cli_args(self, args: Dict[str, any], org: Organization):
        pass

    def get_value(self, workload: pylo.Workload, org: Organization) -> str:
        return_string_list = []

        groups = org.LabelStore.get_label_groups()
        label_group: pylo.LabelGroup
        for label_group in groups:
            for label in label_group.get_members().values():
                label: pylo.Label
                if workload.is_using_label(label):
                    return_string_list.append(label_group.name)
                    break

        return string_list_to_text(return_string_list)


labelGroupOwnershipColum = LabelGroupOwnershipColumn()

if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

if __name__ == "__main__":
    run()



