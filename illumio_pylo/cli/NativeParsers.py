import argparse
from typing import Optional, List, Dict

import illumio_pylo as pylo


class BaseParser:
    _action: Optional[argparse.Action] = None

    def fill_parser(self, parser: argparse.ArgumentParser):
        raise NotImplementedError

    def execute(self, args: str | int, org: 'pylo.Organization', padding: str = '') -> Optional[List['pylo.Label']]:
        raise NotImplementedError

    def get_arg_name(self) -> str:
        return self._action.dest


class LabelParser(BaseParser):
    def __init__(self, action_name: str, action_short_name: Optional[str], label_type: Optional[str] = None, is_required: bool = True, allow_multiple: bool = False, help_text: Optional[str] = None):
        self.action_name = action_name
        self.action_short_name = action_short_name
        self.label_type = label_type
        self.is_required = is_required
        self.allow_multiple = allow_multiple
        self.results: Optional[List['pylo.Label']] = None
        self.results_as_dict_by_href: Dict[str, 'pylo.Label'] = {}
        self.help_text = help_text

    def fill_parser(self, parser: argparse.ArgumentParser):

        help_text = self.help_text
        if help_text is None:
            if self.allow_multiple:
                if self.label_type is None:
                    help_text = f"Filter by label name. Multiple labels can be specified by separating them with a comma."
                else:
                    help_text = f"Filter by label name of type '{self.label_type}'. Multiple labels can be specified by separating them with a comma."
            else:
                if self.label_type is None:
                    help_text = f"Filter by label name."
                else:
                    help_text = f"Filter by label name of type '{self.label_type}'."

        if self.action_short_name is None:
            self._action = parser.add_argument(self.action_name, type=str,
                                               required=self.is_required, help=help_text)
        else:
            self._action = parser.add_argument(self.action_short_name, self.action_name, type=str,
                                               required=self.is_required, help=help_text)

    def execute(self, args: str, org: 'pylo.Organization', padding: str = ''):
        print(f"{padding}{self.action_name}:", end="")
        if args is None:
            print(" None")
            return

        if self.allow_multiple:
            label_names = args.split(",")
        else:
            label_names = [args]

        missing_labels: List[str] = []

        label_objects = org.LabelStore.find_label_by_name(label_names, label_type=self.label_type, missing_labels_names=missing_labels)
        if len(missing_labels) > 0:
            raise pylo.PyloEx(f"Could not find labels: {pylo.string_list_to_text(missing_labels)}, please check their spelling, case and type.")

        # just in case make sure it's a list of unique labels
        self.results = list(set(label_objects))
        self.results_as_dict_by_href = {label.href: label for label in self.results}
        print(f" {pylo.string_list_to_text(self.results)}")


    def filter_workloads_matching_labels(self, workloads: List['pylo.Workload']) -> List['pylo.Workload']:
        if self.results is None:
            return workloads

        # we must group Labels by their type first
        labels_dict_by_type: Dict[str, List[pylo.Label]] = {}
        for label in self.results:
            if labels_dict_by_type.get(label.type) is None:
                labels_dict_by_type[label.type] = [label]
            else:
                labels_dict_by_type[label.type].append(label)

        # now we can filter the workloads, they must match at least one Label per type
        filtered_workloads = []
        for workload in workloads:
            for label_type in labels_dict_by_type:
                if workload.get_label(label_type) in labels_dict_by_type[label_type]:
                    filtered_workloads.append(workload)

        return filtered_workloads

