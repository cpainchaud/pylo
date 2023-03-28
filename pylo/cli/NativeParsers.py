import argparse
from typing import Optional, List, Dict

import pylo


class BaseParser:
    _action: Optional[argparse.Action] = None

    def fill_parser(self, parser: argparse.ArgumentParser):
        raise NotImplementedError

    def execute(self, args: str | int, org: 'pylo.Organization', padding: str = '') -> Optional[List['pylo.Label']]:
        raise NotImplementedError

    def get_arg_name(self) -> str:
        return self._action.dest


class LabelParser(BaseParser):
    def __init__(self, filter_name: str, label_type: Optional[str] = None, is_required: bool = True, is_multiple: bool = False, help_text: Optional[str] = None):
        self.filter_name = filter_name
        self.label_type = label_type
        self.is_required = is_required
        self.is_multiple = is_multiple
        self.results: List['pylo.Label'] = []
        self.results_as_dict_by_href: Dict[str, 'pylo.Label'] = {}
        self.help_text = help_text

    def fill_parser(self, parser: argparse.ArgumentParser):

        help_text = self.help_text
        if help_text is None:
            if self.is_multiple:
                if self.label_type is None:
                    help_text = f"Filter by label name. Multiple labels can be specified by separating them with a comma."
                else:
                    help_text = f"Filter by label name of type '{self.label_type}'. Multiple labels can be specified by separating them with a comma."
            else:
                if self.label_type is None:
                    help_text = f"Filter by label name."
                else:
                    help_text = f"Filter by label name of type '{self.label_type}'."

        self._action = parser.add_argument(self.filter_name, type=str, required=self.is_required,
                                           help=help_text)

    def execute(self, args: str, org: 'pylo.Organization', padding: str = ''):
        print(f"{padding}{self.filter_name}:", end="")
        if args is None:
            print(" None")
            return

        if self.is_multiple:
            label_names = args.split(",")
        else:
            label_names = [args]

        label_objects = []
        for label in label_names:
            if self.label_type is None:
                label_object = org.LabelStore.find_label_by_name_whatever_type(label)
            else:
                label_object = org.LabelStore.find_label_by_name_and_type(label, self.label_type)
            if label_object is None:
                raise Exception(f"Label '{label}' does not exist, make sure there is no typo and check case sensitivity.")
            label_objects.append(label_object)

        # just in case make sure it's a list of unique labels
        self.results = list(set(label_objects))
        self.results_as_dict_by_href = {label.href: label for label in self.results}
        print(f" {pylo.string_list_to_text(self.results)}")
